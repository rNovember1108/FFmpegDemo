#include "ffmpeg_sched.h"
#include "cmdutils.h"
#include "ffmpeg_utils.h"
#include "sync_queue.h"
#include "ffmpeg.h"
#include <chrono>

static const AVClass scheduler_class = {
    .class_name = "Scheduler",
    .version = LIBAVUTIL_VERSION_INT,
};

static const AVClass sch_dec_class = {
    .class_name = "SchDec",
    .version = LIBAVUTIL_VERSION_INT,
    .parent_log_context_offset = offsetof(SchDec, task.func_arg),
};

static const AVClass sch_mux_class = {
    .class_name = "SchMux",
    .version = LIBAVUTIL_VERSION_INT,
    .parent_log_context_offset = offsetof(SchMux, task.func_arg),
};

enum QueueType {
    QUEUE_PACKETS,
    QUEUE_FRAMES,
};

#define SCHEDULE_TOLERANCE (100 * 1000)

Scheduler* sch_alloc(void)
{
    Scheduler* sch = (Scheduler*)av_mallocz(sizeof(*sch));
    if(!sch)
        return NULL;

    sch->av_class = &scheduler_class;
    sch->sdp_auto = 1;
    return sch;
}

static void task_init(Scheduler* sch, SchTask* task, enum SchedulerNodeType type, unsigned idx,
    SchThreadFunc func, void* func_arg)
{
    task->parent = sch;

    task->node.type = type;
    task->node.idx = idx;

    task->func = func;
    task->func_arg = func_arg;
}

static const AVClass sch_demux_class = {
    .class_name = "SchDemux",
    .version = LIBAVUTIL_VERSION_INT,
    .parent_log_context_offset = offsetof(SchDemux, task.func_arg),
};

static int waiter_init(SchWaiter* w)
{
    w->choked.store(0);
    return 0;
}

int sch_add_demux(Scheduler* sch, SchThreadFunc func, void* ctx)
{
    const unsigned idx = sch->nb_demux;

    SchDemux* d;
    int ret;

    int tmp = sch->nb_demux;
    ret = GROW_ARRAY(sch->demux, tmp);
    if(ret < 0)
        return ret;
    sch->nb_demux = tmp;

    d = &sch->demux[idx];

    task_init(sch, &d->task, SCH_NODE_TYPE_DEMUX, idx, func, ctx);

    d->av_class = &sch_demux_class;
    d->send_pkt = av_packet_alloc();
    if(!d->send_pkt)
        return AVERROR(ENOMEM);

    ret = waiter_init(&d->waiter);
    if(ret < 0)
        return ret;

    return idx;
}

static int waiter_wait(Scheduler* sch, SchWaiter* w)
{
    int terminate;

    if(!atomic_load(&w->choked))
        return 0;

    std::unique_lock<std::mutex> lock(w->lock);

    while(atomic_load(&w->choked) && !atomic_load(&sch->terminate))
        w->cond.wait(lock);

    terminate = atomic_load(&sch->terminate);
    return terminate;
}

static int demux_flush(Scheduler* sch, SchDemux* d, AVPacket* pkt)
{
    Timestamp max_end_ts = Timestamp(AV_NOPTS_VALUE , AVRational());

    av_assert0(!pkt->buf && !pkt->data && !pkt->side_data_elems);

    for(unsigned i = 0; i < d->nb_streams; i++) {
        SchDemuxStream* ds = &d->streams[i];

        for(unsigned j = 0; j < ds->nb_dst; j++) {
            const SchedulerNode* dst = &ds->dst[j];
            SchDec* dec;
            int ret;

            if(ds->dst_finished[j] || dst->type != SCH_NODE_TYPE_DEC)
                continue;

            dec = &sch->dec[dst->idx];

            ret = tq_send(dec->queue, 0, pkt);
            if(ret < 0)
                return ret;

            if(dec->queue_end_ts) {
                Timestamp ts;
                ret = av_thread_message_queue_recv(dec->queue_end_ts, &ts, 0);
                if(ret < 0)
                    return ret;

                if(max_end_ts.ts == AV_NOPTS_VALUE ||
                    (ts.ts != AV_NOPTS_VALUE &&
                        av_compare_ts(max_end_ts.ts, max_end_ts.tb, ts.ts, ts.tb) < 0))
                    max_end_ts = ts;

            }
        }
    }

    pkt->pts = max_end_ts.ts;
    pkt->time_base = max_end_ts.tb;

    return 0;
}

static void waiter_set(SchWaiter* w, int choked)
{
    std::unique_lock<std::mutex> lock(w->lock);
    atomic_store(&w->choked, choked);
    w->cond.notify_one();
}

static int64_t trailing_dts(const Scheduler* sch, int count_finished)
{
    int64_t min_dts = INT64_MAX;

    for(unsigned i = 0; i < sch->nb_mux; i++) {
        const SchMux* mux = &sch->mux[i];

        for(unsigned j = 0; j < mux->nb_streams; j++) {
            const SchMuxStream* ms = &mux->streams[j];

            if(ms->source_finished && !count_finished)
                continue;
            if(ms->last_dts == AV_NOPTS_VALUE)
                return AV_NOPTS_VALUE;

            min_dts = FFMIN(min_dts, ms->last_dts);
        }
    }

    return min_dts == INT64_MAX ? AV_NOPTS_VALUE : min_dts;
}

static void schedule_update_locked(Scheduler* sch)
{
    int64_t dts;
    int have_unchoked = 0;

    // on termination request all waiters are choked,
    // we are not to unchoke them
    if(atomic_load(&sch->terminate))
        return;

    dts = trailing_dts(sch, 0);

    atomic_store(&sch->last_dts, dts);

    // initialize our internal state
    for(unsigned type = 0; type < 2; type++)
        for(unsigned i = 0; i < (type ? sch->nb_filters : sch->nb_demux); i++) {
            SchWaiter* w = type ? &sch->filters[i].waiter : &sch->demux[i].waiter;
            w->choked_prev = atomic_load(&w->choked);
            w->choked_next = 1;
        }

    // figure out the sources that are allowed to proceed
    for(unsigned i = 0; i < sch->nb_mux; i++) {
        SchMux* mux = &sch->mux[i];

        for(unsigned j = 0; j < mux->nb_streams; j++) {
            SchMuxStream* ms = &mux->streams[j];
            SchDemux* d;

            // unblock sources for output streams that are not finished
            // and not too far ahead of the trailing stream
            if(ms->source_finished)
                continue;
            if(dts == AV_NOPTS_VALUE && ms->last_dts != AV_NOPTS_VALUE)
                continue;
            if(dts != AV_NOPTS_VALUE && ms->last_dts - dts >= SCHEDULE_TOLERANCE)
                continue;

            // for outputs fed from filtergraphs, consider that filtergraph's
            // best_input information, in other cases there is a well-defined
            // source demuxer
            if(ms->src_sched.type == SCH_NODE_TYPE_FILTER_OUT) {
                SchFilterGraph* fg = &sch->filters[ms->src_sched.idx];
                SchFilterIn* fi;

                // the filtergraph contains internal sources and
                // requested to be scheduled directly
                if(fg->best_input == fg->nb_inputs) {
                    fg->waiter.choked_next = 0;
                    have_unchoked = 1;
                    continue;
                }

                fi = &fg->inputs[fg->best_input];
                d = &sch->demux[fi->src_sched.idx];
            }
            else
                d = &sch->demux[ms->src_sched.idx];

            d->waiter.choked_next = 0;
            have_unchoked = 1;
        }
    }

    // make sure to unchoke at least one source, if still available
    for(unsigned type = 0; !have_unchoked && type < 2; type++)
        for(unsigned i = 0; i < (type ? sch->nb_filters : sch->nb_demux); i++) {
            int exited = type ? sch->filters[i].task_exited : sch->demux[i].task_exited;
            SchWaiter* w = type ? &sch->filters[i].waiter : &sch->demux[i].waiter;
            if(!exited) {
                w->choked_next = 0;
                have_unchoked = 1;
                break;
            }
        }


    for(unsigned type = 0; type < 2; type++)
        for(unsigned i = 0; i < (type ? sch->nb_filters : sch->nb_demux); i++) {
            SchWaiter* w = type ? &sch->filters[i].waiter : &sch->demux[i].waiter;
            if(w->choked_prev != w->choked_next)
                waiter_set(w, w->choked_next);
        }

}

static int mux_queue_packet(SchMux* mux, SchMuxStream* ms, AVPacket* pkt)
{
    PreMuxQueue* q = &ms->pre_mux_queue;
    AVPacket* tmp_pkt = NULL;
    int ret;

    if(!av_fifo_can_write(q->fifo)) {
        size_t     packets = av_fifo_can_read(q->fifo);
        size_t    pkt_size = pkt ? pkt->size : 0;
        int thresh_reached = (q->data_size + pkt_size) > q->data_threshold;
        size_t max_packets = thresh_reached ? q->max_packets : SIZE_MAX;
        size_t new_size = FFMIN(2 * packets, max_packets);

        if(new_size <= packets) {
            av_log(mux, AV_LOG_ERROR,
                "Too many packets buffered for output stream.\n");
            return AVERROR(ENOSPC);
        }
        ret = av_fifo_grow2(q->fifo, new_size - packets);
        if(ret < 0)
            return ret;
    }

    if(pkt) {
        tmp_pkt = av_packet_alloc();
        if(!tmp_pkt)
            return AVERROR(ENOMEM);

        av_packet_move_ref(tmp_pkt, pkt);
        q->data_size += tmp_pkt->size;
    }
    av_fifo_write(q->fifo, &tmp_pkt, 1);

    return 0;
}

static int send_to_mux(Scheduler* sch, SchMux* mux, unsigned stream_idx,
    AVPacket* pkt)
{
    SchMuxStream* ms = &mux->streams[stream_idx];
    int64_t dts = (pkt && pkt->dts != AV_NOPTS_VALUE) ?
        av_rescale_q(pkt->dts + pkt->duration, pkt->time_base, AV_TIME_BASE_Q) :
        AV_NOPTS_VALUE;

    // queue the packet if the muxer cannot be started yet
    if(!atomic_load(&mux->mux_started)) {
        int queued = 0;

        // the muxer could have started between the above atomic check and
        // locking the mutex, then this block falls through to normal send path
        std::unique_lock<std::mutex> lock(sch->mux_ready_lock);

        if(!atomic_load(&mux->mux_started)) {
            int ret = mux_queue_packet(mux, ms, pkt);
            queued = ret < 0 ? ret : 1;
        }

        if(queued < 0)
            return queued;
        else if(queued)
            goto update_schedule;
    }

    if(pkt) {
        int ret;

        if(ms->init_eof)
            return AVERROR_EOF;

        ret = tq_send(mux->queue, stream_idx, pkt);
        if(ret < 0)
            return ret;
    }
    else
        tq_send_finish(mux->queue, stream_idx);

update_schedule:
    // TODO: use atomics to check whether this changes trailing dts
    // to avoid locking unnecesarily
    if(dts != AV_NOPTS_VALUE || !pkt) {
        std::unique_lock<std::mutex> lock(sch->schedule_lock);

        if(pkt) ms->last_dts = dts;
        else     ms->source_finished = 1;

        schedule_update_locked(sch);
    }

    return 0;
}

static int
demux_stream_send_to_dst(Scheduler* sch, const SchedulerNode dst,
    uint8_t* dst_finished, AVPacket* pkt, unsigned flags)
{
    int ret;

    if(*dst_finished)
        return AVERROR_EOF;

    if(pkt && dst.type == SCH_NODE_TYPE_MUX &&
        (flags & DEMUX_SEND_STREAMCOPY_EOF)) {
        av_packet_unref(pkt);
        pkt = NULL;
    }

    if(!pkt)
        goto finish;

    ret = (dst.type == SCH_NODE_TYPE_MUX) ?
        send_to_mux(sch, &sch->mux[dst.idx], dst.idx_stream, pkt) :
        tq_send(sch->dec[dst.idx].queue, 0, pkt);
    if(ret == AVERROR_EOF)
        goto finish;

    return ret;

finish:
    if(dst.type == SCH_NODE_TYPE_MUX)
        send_to_mux(sch, &sch->mux[dst.idx], dst.idx_stream, NULL);
    else
        tq_send_finish(sch->dec[dst.idx].queue, 0);

    *dst_finished = 1;
    return AVERROR_EOF;
}

static int demux_send_for_stream(Scheduler* sch, SchDemux* d, SchDemuxStream* ds,
    AVPacket* pkt, unsigned flags)
{
    unsigned nb_done = 0;

    for(unsigned i = 0; i < ds->nb_dst; i++) {
        AVPacket* to_send = pkt;
        uint8_t* finished = &ds->dst_finished[i];

        int ret;

        // sending a packet consumes it, so make a temporary reference if needed
        if(pkt && i < ds->nb_dst - 1) {
            to_send = d->send_pkt;

            ret = av_packet_ref(to_send, pkt);
            if(ret < 0)
                return ret;
        }

        ret = demux_stream_send_to_dst(sch, ds->dst[i], finished, to_send, flags);
        if(to_send)
            av_packet_unref(to_send);
        if(ret == AVERROR_EOF)
            nb_done++;
        else if(ret < 0)
            return ret;
    }

    return (nb_done == ds->nb_dst) ? AVERROR_EOF : 0;
}

int sch_demux_send(Scheduler* sch, unsigned demux_idx, AVPacket* pkt,
    unsigned flags)
{
    SchDemux* d;
    int terminate;

    av_assert0(demux_idx < sch->nb_demux);
    d = &sch->demux[demux_idx];

    terminate = waiter_wait(sch, &d->waiter);
    if(terminate)
        return AVERROR_EXIT;

    // flush the downstreams after seek
    if(pkt->stream_index == -1)
        return demux_flush(sch, d, pkt);

    av_assert0(pkt->stream_index < d->nb_streams);

    return demux_send_for_stream(sch, d, &d->streams[pkt->stream_index], pkt, flags);
}

int sch_connect(Scheduler* sch, SchedulerNode src, SchedulerNode dst)
{
    int ret;

    switch(src.type) {
    case SCH_NODE_TYPE_DEMUX: {
        SchDemuxStream* ds;

        av_assert0(src.idx < sch->nb_demux &&
            src.idx_stream < sch->demux[src.idx].nb_streams);
        ds = &sch->demux[src.idx].streams[src.idx_stream];

        int temp = ds->nb_dst;
        ret = GROW_ARRAY(ds->dst, temp);
        if(ret < 0)
            return ret;

        ds->nb_dst = temp;
        ds->dst[ds->nb_dst - 1] = dst;

        // demuxed packets go to decoding or streamcopy
        switch(dst.type) {
        case SCH_NODE_TYPE_DEC: {
            SchDec* dec;

            av_assert0(dst.idx < sch->nb_dec);
            dec = &sch->dec[dst.idx];

            av_assert0(!dec->src.type);
            dec->src = src;
            break;
        }
        case SCH_NODE_TYPE_MUX: {
            SchMuxStream* ms;

            av_assert0(dst.idx < sch->nb_mux &&
                dst.idx_stream < sch->mux[dst.idx].nb_streams);
            ms = &sch->mux[dst.idx].streams[dst.idx_stream];

            av_assert0(!ms->src.type);
            ms->src = src;

            break;
        }
        default: av_assert0(0);
        }

        break;
    }
    case SCH_NODE_TYPE_DEC: {
        SchDec* dec;

        av_assert0(src.idx < sch->nb_dec);
        dec = &sch->dec[src.idx];

        int temp = dec->nb_dst;
        ret = GROW_ARRAY(dec->dst, temp);
        if(ret < 0)
            return ret;

        dec->nb_dst = temp;
        dec->dst[dec->nb_dst - 1] = dst;

        // decoded frames go to filters or encoding
        switch(dst.type) {
        case SCH_NODE_TYPE_FILTER_IN: {
            SchFilterIn* fi;

            av_assert0(dst.idx < sch->nb_filters &&
                dst.idx_stream < sch->filters[dst.idx].nb_inputs);
            fi = &sch->filters[dst.idx].inputs[dst.idx_stream];

            av_assert0(!fi->src.type);
            fi->src = src;
            break;
        }
        case SCH_NODE_TYPE_ENC: {
            SchEnc* enc;

            av_assert0(dst.idx < sch->nb_enc);
            enc = &sch->enc[dst.idx];

            av_assert0(!enc->src.type);
            enc->src = src;
            break;
        }
        default: av_assert0(0);
        }

        break;
    }
    case SCH_NODE_TYPE_FILTER_OUT: {
        SchFilterOut* fo;
        SchEnc* enc;

        av_assert0(src.idx < sch->nb_filters &&
            src.idx_stream < sch->filters[src.idx].nb_outputs);
        // filtered frames go to encoding
        av_assert0(dst.type == SCH_NODE_TYPE_ENC &&
            dst.idx < sch->nb_enc);

        fo = &sch->filters[src.idx].outputs[src.idx_stream];
        enc = &sch->enc[dst.idx];

        av_assert0(!fo->dst.type && !enc->src.type);
        fo->dst = dst;
        enc->src = src;

        break;
    }
    case SCH_NODE_TYPE_ENC: {
        SchEnc* enc;
        SchMuxStream* ms;

        av_assert0(src.idx < sch->nb_enc);
        // encoding packets go to muxing
        av_assert0(dst.type == SCH_NODE_TYPE_MUX &&
            dst.idx < sch->nb_mux &&
            dst.idx_stream < sch->mux[dst.idx].nb_streams);
        enc = &sch->enc[src.idx];
        ms = &sch->mux[dst.idx].streams[dst.idx_stream];

        av_assert0(!enc->dst.type && !ms->src.type);
        enc->dst = dst;
        ms->src = src;

        break;
    }
    default: av_assert0(0);
    }

    return 0;
}

int sch_add_demux_stream(Scheduler* sch, unsigned demux_idx)
{
    SchDemux* d;
    int ret;

    av_assert0(demux_idx < sch->nb_demux);
    d = &sch->demux[demux_idx];

    int temp = d->nb_streams;
    ret = GROW_ARRAY(d->streams, temp);
    d->nb_streams = temp;
    return ret < 0 ? ret : d->nb_streams - 1;
}

static int queue_alloc(ThreadQueue** ptq, unsigned nb_streams, unsigned queue_size,
    enum QueueType type)
{
    ThreadQueue* tq;
    ObjPool* op;

    op = (type == QUEUE_PACKETS) ? objpool_alloc_packets() :
        objpool_alloc_frames();
    if(!op)
        return AVERROR(ENOMEM);

    tq = tq_alloc(nb_streams, queue_size, op,
        (type == QUEUE_PACKETS) ? pkt_move : frame_move);
    if(!tq) {
        objpool_free(&op);
        return AVERROR(ENOMEM);
    }

    *ptq = tq;
    return 0;
}

int sch_add_dec(Scheduler* sch, SchThreadFunc func, void* ctx,
    int send_end_ts)
{
    const unsigned idx = sch->nb_dec;

    SchDec* dec;
    int ret;

    int temp = sch->nb_dec;
    ret = GROW_ARRAY(sch->dec, temp);
    if(ret < 0)
        return ret;
    sch->nb_dec = temp;
    dec = &sch->dec[idx];

    task_init(sch, &dec->task, SCH_NODE_TYPE_DEC, idx, func, ctx);

    dec->av_class = &sch_dec_class;
    dec->send_frame = av_frame_alloc();
    if(!dec->send_frame)
        return AVERROR(ENOMEM);

    ret = queue_alloc(&dec->queue, 1, 1, QUEUE_PACKETS);
    if(ret < 0)
        return ret;

    if(send_end_ts) {
        ret = av_thread_message_queue_alloc(&dec->queue_end_ts, 1, sizeof(Timestamp));
        if(ret < 0)
            return ret;
    }

    return idx;
}

int sch_dec_receive(Scheduler* sch, unsigned dec_idx, AVPacket* pkt)
{
    SchDec* dec;
    int ret, dummy;

    av_assert0(dec_idx < sch->nb_dec);
    dec = &sch->dec[dec_idx];

    // the decoder should have given us post-flush end timestamp in pkt
    if(dec->expect_end_ts) {
        Timestamp ts = Timestamp(pkt->pts, pkt->time_base);
        ret = av_thread_message_queue_send(dec->queue_end_ts, &ts, 0);
        if(ret < 0)
            return ret;

        dec->expect_end_ts = 0;
    }

    ret = tq_receive(dec->queue, &dummy, pkt);
    av_assert0(dummy <= 0);

    // got a flush packet, on the next call to this function the decoder
    // will give us post-flush end timestamp
    if(ret >= 0 && !pkt->data && !pkt->side_data_elems && dec->queue_end_ts)
        dec->expect_end_ts = 1;

    return ret;
}

static int send_to_filter(Scheduler* sch, SchFilterGraph* fg,
    unsigned in_idx, AVFrame* frame)
{
    if(frame)
        return tq_send(fg->queue, in_idx, frame);

    if(!fg->inputs[in_idx].send_finished) {
        fg->inputs[in_idx].send_finished = 1;
        tq_send_finish(fg->queue, in_idx);

        // close the control stream when all actual inputs are done
        if(atomic_fetch_add(&fg->nb_inputs_finished, 1) == fg->nb_inputs - 1)
            tq_send_finish(fg->queue, fg->nb_inputs);
    }
    return 0;
}

static int enc_open(Scheduler* sch, SchEnc* enc, const AVFrame* frame)
{
    int ret;

    ret = enc->open_cb(enc->task.func_arg, frame);
    if(ret < 0)
        return ret;

    // ret>0 signals audio frame size, which means sync queue must
    // have been enabled during encoder creation
    if(ret > 0) {
        SchSyncQueue* sq;

        av_assert0(enc->sq_idx[0] >= 0);
        sq = &sch->sq_enc[enc->sq_idx[0]];

        std::unique_lock<std::mutex> lock(sq->lock);
        sq_frame_samples(sq->sq, enc->sq_idx[1], ret);
    }

    return 0;
}

static int send_to_enc_thread(Scheduler* sch, SchEnc* enc, AVFrame* frame)
{
    int ret;

    if(!frame) {
        tq_send_finish(enc->queue, 0);
        return 0;
    }

    if(enc->in_finished)
        return AVERROR_EOF;

    ret = tq_send(enc->queue, 0, frame);
    if(ret < 0)
        enc->in_finished = 1;

    return ret;
}

static int send_to_enc_sq(Scheduler* sch, SchEnc* enc, AVFrame* frame)
{
    SchSyncQueue* sq = &sch->sq_enc[enc->sq_idx[0]];
    int ret = 0;

    // inform the scheduling code that no more input will arrive along this path;
    // this is necessary because the sync queue may not send an EOF downstream
    // until other streams finish
    // TODO: consider a cleaner way of passing this information through
    //       the pipeline
    if(!frame) {
        SchMux* mux = &sch->mux[enc->dst.idx];
        SchMuxStream* ms = &mux->streams[enc->dst.idx_stream];

        std::unique_lock<std::mutex> lock(sch->schedule_lock);
        ms->source_finished = 1;
        schedule_update_locked(sch);
    }

    std::unique_lock<std::mutex> lock1(sq->lock);
    ret = sq_send(sq->sq, enc->sq_idx[1], SQFRAME(frame));
    if(ret < 0)
        goto finish;

    while(1) {
        SchEnc* enc;

        // TODO: the SQ API should be extended to allow returning EOF
        // for individual streams
        ret = sq_receive(sq->sq, -1, SQFRAME(sq->frame));
        if(ret == AVERROR(EAGAIN)) {
            ret = 0;
            goto finish;
        }
        else if(ret < 0) {
            // close all encoders fed from this sync queue
            for(unsigned i = 0; i < sq->nb_enc_idx; i++) {
                int err = send_to_enc_thread(sch, &sch->enc[sq->enc_idx[i]], NULL);

                // if the sync queue error is EOF and closing the encoder
                // produces a more serious error, make sure to pick the latter
                ret = err_merge((ret == AVERROR_EOF && err < 0) ? 0 : ret, err);
            }
            goto finish;
        }

        enc = &sch->enc[sq->enc_idx[ret]];
        ret = send_to_enc_thread(sch, enc, sq->frame);
        if(ret < 0) {
            av_assert0(ret == AVERROR_EOF);
            av_frame_unref(sq->frame);
            sq_send(sq->sq, enc->sq_idx[1], SQFRAME(NULL));
            continue;
        }
    }

finish:
    return ret;
}

static int send_to_enc(Scheduler* sch, SchEnc* enc, AVFrame* frame)
{
    if(enc->open_cb && frame && !enc->opened) {
        int ret = enc_open(sch, enc, frame);
        if(ret < 0)
            return ret;
        enc->opened = 1;

        // discard empty frames that only carry encoder init parameters
        if(!frame->buf[0]) {
            av_frame_unref(frame);
            return 0;
        }
    }

    return (enc->sq_idx[0] >= 0) ?
        send_to_enc_sq(sch, enc, frame) :
        send_to_enc_thread(sch, enc, frame);
}

static int dec_send_to_dst(Scheduler* sch, const SchedulerNode dst,
    uint8_t* dst_finished, AVFrame* frame)
{
    int ret;

    if(*dst_finished)
        return AVERROR_EOF;

    if(!frame)
        goto finish;

    ret = (dst.type == SCH_NODE_TYPE_FILTER_IN) ?
        send_to_filter(sch, &sch->filters[dst.idx], dst.idx_stream, frame) :
        send_to_enc(sch, &sch->enc[dst.idx], frame);
    if(ret == AVERROR_EOF)
        goto finish;

    return ret;

finish:
    if(dst.type == SCH_NODE_TYPE_FILTER_IN)
        send_to_filter(sch, &sch->filters[dst.idx], dst.idx_stream, NULL);
    else
        send_to_enc(sch, &sch->enc[dst.idx], NULL);

    *dst_finished = 1;

    return AVERROR_EOF;
}



int sch_dec_send(Scheduler* sch, unsigned dec_idx, AVFrame* frame)
{
    SchDec* dec;
    int ret = 0;
    unsigned nb_done = 0;

    av_assert0(dec_idx < sch->nb_dec);
    dec = &sch->dec[dec_idx];

    for(unsigned i = 0; i < dec->nb_dst; i++) {
        uint8_t* finished = &dec->dst_finished[i];
        AVFrame* to_send = frame;

        // sending a frame consumes it, so make a temporary reference if needed
        if(i < dec->nb_dst - 1) {
            to_send = dec->send_frame;

            // frame may sometimes contain props only,
            // e.g. to signal EOF timestamp
            ret = frame->buf[0] ? av_frame_ref(to_send, frame) :
                av_frame_copy_props(to_send, frame);
            if(ret < 0)
                return ret;
        }

        ret = dec_send_to_dst(sch, dec->dst[i], finished, to_send);
        if(ret < 0) {
            av_frame_unref(to_send);
            if(ret == AVERROR_EOF) {
                nb_done++;
                ret = 0;
                continue;
            }
            goto finish;
        }
    }

finish:
    return ret < 0 ? ret :
        (nb_done == dec->nb_dst) ? AVERROR_EOF : 0;
}

int sch_add_mux(Scheduler* sch, SchThreadFunc func, int (*init)(void*),
    void* arg, int sdp_auto)
{
    const unsigned idx = sch->nb_mux;

    SchMux* mux;
    int ret;

    int temp = sch->nb_mux;
    ret = GROW_ARRAY(sch->mux, temp);
    if(ret < 0)
        return ret;
    sch->nb_mux = temp;
    mux = &sch->mux[idx];
    mux->av_class = &sch_mux_class;
    mux->init = init;

    task_init(sch, &mux->task, SCH_NODE_TYPE_MUX, idx, func, arg);

    sch->sdp_auto &= sdp_auto;

    return idx;
}

int sch_mux_receive(Scheduler* sch, unsigned mux_idx, AVPacket* pkt)
{
    SchMux* mux;
    int ret, stream_idx;

    av_assert0(mux_idx < sch->nb_mux);
    mux = &sch->mux[mux_idx];

    ret = tq_receive(mux->queue, &stream_idx, pkt);
    pkt->stream_index = stream_idx;
    return ret;
}

int sch_mux_sub_heartbeat(Scheduler* sch, unsigned mux_idx, unsigned stream_idx,
    const AVPacket* pkt)
{
    SchMux* mux;
    SchMuxStream* ms;

    av_assert0(mux_idx < sch->nb_mux);
    mux = &sch->mux[mux_idx];

    av_assert0(stream_idx < mux->nb_streams);
    ms = &mux->streams[stream_idx];

    for(unsigned i = 0; i < ms->nb_sub_heartbeat_dst; i++) {
        SchDec* dst = &sch->dec[ms->sub_heartbeat_dst[i]];
        int ret;

        ret = av_packet_copy_props(mux->sub_heartbeat_pkt, pkt);
        if(ret < 0)
            return ret;

        tq_send(dst->queue, 0, mux->sub_heartbeat_pkt);
    }

    return 0;
}

void sch_mux_receive_finish(Scheduler* sch, unsigned mux_idx, unsigned stream_idx)
{
    SchMux* mux;

    av_assert0(mux_idx < sch->nb_mux);
    mux = &sch->mux[mux_idx];

    av_assert0(stream_idx < mux->nb_streams);
    tq_receive_finish(mux->queue, stream_idx);

    std::unique_lock<std::mutex> lock(sch->schedule_lock);
    mux->streams[stream_idx].source_finished = 1;
    schedule_update_locked(sch);
}

int sch_add_mux_stream(Scheduler* sch, unsigned mux_idx)
{
    SchMux* mux;
    SchMuxStream* ms;
    unsigned      stream_idx;
    int ret;

    av_assert0(mux_idx < sch->nb_mux);
    mux = &sch->mux[mux_idx];

    int temp = mux->nb_streams;
    ret = GROW_ARRAY(mux->streams, temp);
    if(ret < 0)
        return ret;
    mux->nb_streams = temp;
    stream_idx = mux->nb_streams - 1;

    ms = &mux->streams[stream_idx];

    ms->pre_mux_queue.fifo = av_fifo_alloc2(8, sizeof(AVPacket*), 0);
    if(!ms->pre_mux_queue.fifo)
        return AVERROR(ENOMEM);

    ms->last_dts = AV_NOPTS_VALUE;

    return stream_idx;
}

static const AVClass sch_enc_class = {
    .class_name = "SchEnc",
    .version = LIBAVUTIL_VERSION_INT,
    .parent_log_context_offset = offsetof(SchEnc, task.func_arg),
};

int sch_add_enc(Scheduler* sch, SchThreadFunc func, void* ctx,
    int (*open_cb)(void* opaque, const AVFrame* frame))
{
    const unsigned idx = sch->nb_enc;

    SchEnc* enc;
    int ret;

    int temp = sch->nb_enc;
    ret = GROW_ARRAY(sch->enc, temp);
    if(ret < 0)
        return ret;
    sch->nb_enc = temp;
    enc = &sch->enc[idx];

    enc->av_class = &sch_enc_class;
    enc->open_cb = open_cb;
    enc->sq_idx[0] = -1;
    enc->sq_idx[1] = -1;

    task_init(sch, &enc->task, SCH_NODE_TYPE_ENC, idx, func, ctx);

    ret = queue_alloc(&enc->queue, 1, 1, QUEUE_FRAMES);
    if(ret < 0)
        return ret;

    return idx;
}

static int demux_done(Scheduler* sch, unsigned demux_idx)
{
    SchDemux* d = &sch->demux[demux_idx];
    int ret = 0;

    for(unsigned i = 0; i < d->nb_streams; i++) {
        int err = demux_send_for_stream(sch, d, &d->streams[i], NULL, 0);
        if(err != AVERROR_EOF)
            ret = err_merge(ret, err);
    }

    std::unique_lock<std::mutex> lock(sch->schedule_lock);
    d->task_exited = 1;
    schedule_update_locked(sch);
    return ret;
}

static int mux_done(Scheduler* sch, unsigned mux_idx)
{
    SchMux* mux = &sch->mux[mux_idx];

    std::unique_lock<std::mutex> lock1(sch->schedule_lock);
    for(unsigned i = 0; i < mux->nb_streams; i++) {
        tq_receive_finish(mux->queue, i);
        mux->streams[i].source_finished = 1;
    }

    schedule_update_locked(sch);

    std::unique_lock<std::mutex> lock2(sch->mux_done_lock);
    av_assert0(sch->nb_mux_done < sch->nb_mux);
    sch->nb_mux_done++;

    sch->mux_done_cond.notify_one();
    return 0;
}

static int enc_done(Scheduler* sch, unsigned enc_idx)
{
    SchEnc* enc = &sch->enc[enc_idx];

    tq_receive_finish(enc->queue, 0);

    return send_to_mux(sch, &sch->mux[enc->dst.idx], enc->dst.idx_stream, NULL);
}

static int dec_done(Scheduler* sch, unsigned dec_idx)
{
    SchDec* dec = &sch->dec[dec_idx];
    int ret = 0;

    tq_receive_finish(dec->queue, 0);

    // make sure our source does not get stuck waiting for end timestamps
    // that will never arrive
    if(dec->queue_end_ts)
        av_thread_message_queue_set_err_recv(dec->queue_end_ts, AVERROR_EOF);

    for(unsigned i = 0; i < dec->nb_dst; i++) {
        int err = dec_send_to_dst(sch, dec->dst[i], &dec->dst_finished[i], NULL);
        if(err < 0 && err != AVERROR_EOF)
            ret = err_merge(ret, err);
    }

    return ret;
}

static int filter_done(Scheduler* sch, unsigned fg_idx)
{
    SchFilterGraph* fg = &sch->filters[fg_idx];
    int ret = 0;

    for(unsigned i = 0; i <= fg->nb_inputs; i++)
        tq_receive_finish(fg->queue, i);

    for(unsigned i = 0; i < fg->nb_outputs; i++) {
        SchEnc* enc = &sch->enc[fg->outputs[i].dst.idx];
        int err = send_to_enc(sch, enc, NULL);
        if(err < 0 && err != AVERROR_EOF)
            ret = err_merge(ret, err);
    }

    std::unique_lock<std::mutex> lock(sch->schedule_lock);
    fg->task_exited = 1;
    schedule_update_locked(sch);
    return ret;
}

static void* task_wrapper(void* arg)
{
    SchTask* task = (SchTask*)arg;
    Scheduler* sch = task->parent;
    int ret;
    int err = 0;

    ret = (intptr_t)task->func(task->func_arg);
    if(ret < 0)
        av_log(task->func_arg, AV_LOG_ERROR,
            "Task finished with error code: %d (%s)\n", ret, av_err2str(ret));

    switch(task->node.type) {
    case SCH_NODE_TYPE_DEMUX:       err = demux_done(sch, task->node.idx); break;
    case SCH_NODE_TYPE_MUX:         err = mux_done(sch, task->node.idx); break;
    case SCH_NODE_TYPE_DEC:         err = dec_done(sch, task->node.idx); break;
    case SCH_NODE_TYPE_ENC:         err = enc_done(sch, task->node.idx); break;
    case SCH_NODE_TYPE_FILTER_IN:   err = filter_done(sch, task->node.idx); break;
    default: av_assert0(0);
    }

    ret = err_merge(ret, err);

    // EOF is considered normal termination
    if(ret == AVERROR_EOF)
        ret = 0;
    if(ret < 0)
        atomic_store(&sch->task_failed, 1);

    av_log(task->func_arg, ret < 0 ? AV_LOG_ERROR : AV_LOG_VERBOSE,
        "Terminating thread with return code %d (%s)\n", ret,
        ret < 0 ? av_err2str(ret) : "success");

    return (void*)(intptr_t)ret;
}

static int task_start(SchTask* task)
{
    int ret = 0;

    av_log(task->func_arg, AV_LOG_VERBOSE, "Starting thread...\n");
    av_assert0(!task->thread_running);

    task->thread = std::thread(task_wrapper, task);
    if(!task->thread.joinable()) {
        av_log(task->func_arg, AV_LOG_ERROR, "pthread_create() failed: %s\n",
            strerror(ret));
        return AVERROR(ret);
    }

    task->thread_running = 1;
    return 0;
}

static int mux_task_start(SchMux* mux)
{
    int ret = 0;

    ret = task_start(&mux->task);
    if(ret < 0)
        return ret;

    /* flush the pre-muxing queues */
    for(unsigned i = 0; i < mux->nb_streams; i++) {
        SchMuxStream* ms = &mux->streams[i];
        AVPacket* pkt;

        while(av_fifo_read(ms->pre_mux_queue.fifo, &pkt, 1) >= 0) {
            if(pkt) {
                if(!ms->init_eof)
                    ret = tq_send(mux->queue, i, pkt);
                av_packet_free(&pkt);
                if(ret == AVERROR_EOF)
                    ms->init_eof = 1;
                else if(ret < 0)
                    return ret;
            }
            else
                tq_send_finish(mux->queue, i);
        }
    }

    atomic_store(&mux->mux_started, 1);

    return 0;
}

static int mux_init(Scheduler* sch, SchMux* mux)
{
    int ret;

    ret = mux->init(mux->task.func_arg);
    if(ret < 0)
        return ret;

    sch->nb_mux_ready++;

    if(sch->sdp_filename || sch->sdp_auto) {
        if(sch->nb_mux_ready < sch->nb_mux)
            return 0;

        ret = print_sdp(sch->sdp_filename);
        if(ret < 0) {
            av_log(sch, AV_LOG_ERROR, "Error writing the SDP.\n");
            return ret;
        }

        /* SDP is written only after all the muxers are ready, so now we
         * start ALL the threads */
        for(unsigned i = 0; i < sch->nb_mux; i++) {
            ret = mux_task_start(&sch->mux[i]);
            if(ret < 0)
                return ret;
        }
    }
    else {
        ret = mux_task_start(mux);
        if(ret < 0)
            return ret;
    }

    return 0;
}

int sch_mux_stream_ready(Scheduler* sch, unsigned mux_idx, unsigned stream_idx)
{
    SchMux* mux;
    int ret = 0;

    av_assert0(mux_idx < sch->nb_mux);
    mux = &sch->mux[mux_idx];

    av_assert0(stream_idx < mux->nb_streams);

    std::unique_lock<std::mutex> lock(sch->mux_ready_lock);
    av_assert0(mux->nb_streams_ready < mux->nb_streams);

    // this may be called during initialization - do not start
    // threads before sch_start() is called
    if(++mux->nb_streams_ready == mux->nb_streams && sch->transcode_started)
        ret = mux_init(sch, mux);

    return ret;
}

int sch_enc_receive(Scheduler* sch, unsigned enc_idx, AVFrame* frame)
{
    SchEnc* enc;
    int ret, dummy;

    av_assert0(enc_idx < sch->nb_enc);
    enc = &sch->enc[enc_idx];

    ret = tq_receive(enc->queue, &dummy, frame);
    av_assert0(dummy <= 0);

    return ret;
}

int sch_enc_send(Scheduler* sch, unsigned enc_idx, AVPacket* pkt)
{
    SchEnc* enc;
    int ret;

    av_assert0(enc_idx < sch->nb_enc);
    enc = &sch->enc[enc_idx];

    if(enc->out_finished)
        return pkt ? AVERROR_EOF : 0;

    ret = send_to_mux(sch, &sch->mux[enc->dst.idx], enc->dst.idx_stream, pkt);
    if(ret < 0)
        enc->out_finished = 1;

    return ret;
}

int sch_mux_sub_heartbeat_add(Scheduler* sch, unsigned mux_idx, unsigned stream_idx,
    unsigned dec_idx)
{
    SchMux* mux;
    SchMuxStream* ms;
    int ret = 0;

    av_assert0(mux_idx < sch->nb_mux);
    mux = &sch->mux[mux_idx];

    av_assert0(stream_idx < mux->nb_streams);
    ms = &mux->streams[stream_idx];

    int temp = ms->nb_sub_heartbeat_dst;
    ret = GROW_ARRAY(ms->sub_heartbeat_dst, temp);
    if(ret < 0)
        return ret;

    ms->nb_sub_heartbeat_dst = temp;
    av_assert0(dec_idx < sch->nb_dec);
    ms->sub_heartbeat_dst[ms->nb_sub_heartbeat_dst - 1] = dec_idx;

    if(!mux->sub_heartbeat_pkt) {
        mux->sub_heartbeat_pkt = av_packet_alloc();
        if(!mux->sub_heartbeat_pkt)
            return AVERROR(ENOMEM);
    }

    return 0;
}

int sch_add_sq_enc(Scheduler* sch, uint64_t buf_size_us, void* logctx)
{
    SchSyncQueue* sq;
    int ret;

    int temp = sch->nb_sq_enc;
    ret = GROW_ARRAY(sch->sq_enc, temp);
    if(ret < 0)
        return ret;
    sch->nb_sq_enc = temp;
    sq = &sch->sq_enc[sch->nb_sq_enc - 1];

    sq->sq = sq_alloc(SYNC_QUEUE_FRAMES, buf_size_us, logctx);
    if(!sq->sq)
        return AVERROR(ENOMEM);

    sq->frame = av_frame_alloc();
    if(!sq->frame)
        return AVERROR(ENOMEM);

    return sq - sch->sq_enc;
}

int sch_sq_add_enc(Scheduler* sch, unsigned sq_idx, unsigned enc_idx,
    int limiting, uint64_t max_frames)
{
    SchSyncQueue* sq;
    SchEnc* enc;
    int ret;

    av_assert0(sq_idx < sch->nb_sq_enc);
    sq = &sch->sq_enc[sq_idx];

    av_assert0(enc_idx < sch->nb_enc);
    enc = &sch->enc[enc_idx];

    int temp = sq->nb_enc_idx;
    ret = GROW_ARRAY(sq->enc_idx, temp);
    if(ret < 0)
        return ret;
    sq->nb_enc_idx = temp;
    sq->enc_idx[sq->nb_enc_idx - 1] = enc_idx;

    ret = sq_add_stream(sq->sq, limiting);
    if(ret < 0)
        return ret;

    enc->sq_idx[0] = sq_idx;
    enc->sq_idx[1] = ret;

    if(max_frames != INT64_MAX)
        sq_limit_frames(sq->sq, enc->sq_idx[1], max_frames);

    return 0;
}

void sch_mux_stream_buffering(Scheduler* sch, unsigned mux_idx, unsigned stream_idx,
    size_t data_threshold, int max_packets)
{
    SchMux* mux;
    SchMuxStream* ms;

    av_assert0(mux_idx < sch->nb_mux);
    mux = &sch->mux[mux_idx];

    av_assert0(stream_idx < mux->nb_streams);
    ms = &mux->streams[stream_idx];

    ms->pre_mux_queue.max_packets = max_packets;
    ms->pre_mux_queue.data_threshold = data_threshold;
}

static const AVClass sch_fg_class = {
    .class_name = "SchFilterGraph",
    .version = LIBAVUTIL_VERSION_INT,
    .parent_log_context_offset = offsetof(SchFilterGraph, task.func_arg),
};

int sch_add_filtergraph(Scheduler* sch, unsigned nb_inputs, unsigned nb_outputs,
    SchThreadFunc func, void* ctx)
{
    const unsigned idx = sch->nb_filters;

    SchFilterGraph* fg;
    int ret;

    int temp = sch->nb_filters;
    ret = GROW_ARRAY(sch->filters, temp);
    if(ret < 0)
        return ret;
    sch->nb_filters = temp;
    fg = &sch->filters[idx];

    fg->av_class = &sch_fg_class;

    task_init(sch, &fg->task, SCH_NODE_TYPE_FILTER_IN, idx, func, ctx);

    if(nb_inputs) {
        fg->inputs = (SchFilterIn*)av_calloc(nb_inputs, sizeof(*fg->inputs));
        if(!fg->inputs)
            return AVERROR(ENOMEM);
        fg->nb_inputs = nb_inputs;
    }

    if(nb_outputs) {
        fg->outputs = (SchFilterOut*)av_calloc(nb_outputs, sizeof(*fg->outputs));
        if(!fg->outputs)
            return AVERROR(ENOMEM);
        fg->nb_outputs = nb_outputs;
    }

    ret = waiter_init(&fg->waiter);
    if(ret < 0)
        return ret;

    ret = queue_alloc(&fg->queue, fg->nb_inputs + 1, 1, QUEUE_FRAMES);
    if(ret < 0)
        return ret;

    return idx;
}

int sch_filter_receive(Scheduler* sch, unsigned fg_idx,
    unsigned* in_idx, AVFrame* frame)
{
    SchFilterGraph* fg;

    av_assert0(fg_idx < sch->nb_filters);
    fg = &sch->filters[fg_idx];

    av_assert0(*in_idx <= fg->nb_inputs);

    // update scheduling to account for desired input stream, if it changed
    //
    // this check needs no locking because only the filtering thread
    // updates this value
    if(*in_idx != fg->best_input) {
        std::unique_lock<std::mutex> lock(sch->schedule_lock);
        fg->best_input = *in_idx;
        schedule_update_locked(sch);
    }

    if(*in_idx == fg->nb_inputs) {
        int terminate = waiter_wait(sch, &fg->waiter);
        return terminate ? AVERROR_EOF : AVERROR(EAGAIN);
    }

    while(1) {
        int ret, idx;

        ret = tq_receive(fg->queue, &idx, frame);
        if(idx < 0)
            return AVERROR_EOF;
        else if(ret >= 0) {
            *in_idx = idx;
            return 0;
        }

        // disregard EOFs for specific streams - they should always be
        // preceded by an EOF frame
    }
}

int sch_filter_send(Scheduler* sch, unsigned fg_idx, unsigned out_idx, AVFrame* frame)
{
    SchFilterGraph* fg;

    av_assert0(fg_idx < sch->nb_filters);
    fg = &sch->filters[fg_idx];

    av_assert0(out_idx < fg->nb_outputs);
    return send_to_enc(sch, &sch->enc[fg->outputs[out_idx].dst.idx], frame);
}

int sch_start(Scheduler* sch)
{
    int ret;

    sch->transcode_started = 1;

    for(unsigned i = 0; i < sch->nb_mux; i++) {
        SchMux* mux = &sch->mux[i];

        for(unsigned j = 0; j < mux->nb_streams; j++) {
            SchMuxStream* ms = &mux->streams[j];

            switch(ms->src.type) {
            case SCH_NODE_TYPE_ENC: {
                SchEnc* enc = &sch->enc[ms->src.idx];
                if(enc->src.type == SCH_NODE_TYPE_DEC) {
                    ms->src_sched = sch->dec[enc->src.idx].src;
                    av_assert0(ms->src_sched.type == SCH_NODE_TYPE_DEMUX);
                }
                else {
                    ms->src_sched = enc->src;
                    av_assert0(ms->src_sched.type == SCH_NODE_TYPE_FILTER_OUT);
                }
                break;
            }
            case SCH_NODE_TYPE_DEMUX:
                ms->src_sched = ms->src;
                break;
            default:
                av_log(mux, AV_LOG_ERROR,
                    "Muxer stream #%u not connected to a source\n", j);
                return AVERROR(EINVAL);
            }
        }

        ret = queue_alloc(&mux->queue, mux->nb_streams, 1, QUEUE_PACKETS);
        if(ret < 0)
            return ret;

        if(mux->nb_streams_ready == mux->nb_streams) {
            ret = mux_init(sch, mux);
            if(ret < 0)
                return ret;
        }
    }

    for(unsigned i = 0; i < sch->nb_enc; i++) {
        SchEnc* enc = &sch->enc[i];

        if(!enc->src.type) {
            av_log(enc, AV_LOG_ERROR,
                "Encoder not connected to a source\n");
            return AVERROR(EINVAL);
        }
        if(!enc->dst.type) {
            av_log(enc, AV_LOG_ERROR,
                "Encoder not connected to a sink\n");
            return AVERROR(EINVAL);
        }

        ret = task_start(&enc->task);
        if(ret < 0)
            return ret;
    }

    for(unsigned i = 0; i < sch->nb_filters; i++) {
        SchFilterGraph* fg = &sch->filters[i];

        for(unsigned j = 0; j < fg->nb_inputs; j++) {
            SchFilterIn* fi = &fg->inputs[j];

            if(!fi->src.type) {
                av_log(fg, AV_LOG_ERROR,
                    "Filtergraph input %u not connected to a source\n", j);
                return AVERROR(EINVAL);
            }

            fi->src_sched = sch->dec[fi->src.idx].src;
        }

        for(unsigned j = 0; j < fg->nb_outputs; j++) {
            SchFilterOut* fo = &fg->outputs[j];

            if(!fo->dst.type) {
                av_log(fg, AV_LOG_ERROR,
                    "Filtergraph %u output %u not connected to a sink\n", i, j);
                return AVERROR(EINVAL);
            }
        }

        ret = task_start(&fg->task);
        if(ret < 0)
            return ret;
    }

    for(unsigned i = 0; i < sch->nb_dec; i++) {
        SchDec* dec = &sch->dec[i];

        if(!dec->src.type) {
            av_log(dec, AV_LOG_ERROR,
                "Decoder not connected to a source\n");
            return AVERROR(EINVAL);
        }
        if(!dec->nb_dst) {
            av_log(dec, AV_LOG_ERROR,
                "Decoder not connected to any sink\n");
            return AVERROR(EINVAL);
        }

        dec->dst_finished = (uint8_t*)av_calloc(dec->nb_dst, sizeof(*dec->dst_finished));
        if(!dec->dst_finished)
            return AVERROR(ENOMEM);

        ret = task_start(&dec->task);
        if(ret < 0)
            return ret;
    }

    for(unsigned i = 0; i < sch->nb_demux; i++) {
        SchDemux* d = &sch->demux[i];

        if(!d->nb_streams)
            continue;

        for(unsigned j = 0; j < d->nb_streams; j++) {
            SchDemuxStream* ds = &d->streams[j];

            if(!ds->nb_dst) {
                av_log(d, AV_LOG_ERROR,
                    "Demuxer stream %u not connected to any sink\n", j);
                return AVERROR(EINVAL);
            }

            ds->dst_finished = (uint8_t*)av_calloc(ds->nb_dst, sizeof(*ds->dst_finished));
            if(!ds->dst_finished)
                return AVERROR(ENOMEM);
        }

        ret = task_start(&d->task);
        if(ret < 0)
            return ret;
    }

    std::unique_lock<std::mutex> lock(sch->schedule_lock);
    schedule_update_locked(sch);
    return 0;
}

static int task_stop(SchTask* task)
{
    if(!task->thread_running)
        return 0;

    task->thread.join();
    task->thread_running = 0;

    return 0;
}

int sch_stop(Scheduler* sch, int64_t* finish_ts)
{
    int ret = 0, err;

    atomic_store(&sch->terminate, 1);

    for(unsigned type = 0; type < 2; type++)
        for(unsigned i = 0; i < (type ? sch->nb_demux : sch->nb_filters); i++) {
            SchWaiter* w = type ? &sch->demux[i].waiter : &sch->filters[i].waiter;
            waiter_set(w, 1);
        }

    for(unsigned i = 0; i < sch->nb_demux; i++) {
        SchDemux* d = &sch->demux[i];

        err = task_stop(&d->task);
        ret = err_merge(ret, err);
    }

    for(unsigned i = 0; i < sch->nb_dec; i++) {
        SchDec* dec = &sch->dec[i];

        err = task_stop(&dec->task);
        ret = err_merge(ret, err);
    }

    for(unsigned i = 0; i < sch->nb_filters; i++) {
        SchFilterGraph* fg = &sch->filters[i];

        err = task_stop(&fg->task);
        ret = err_merge(ret, err);
    }

    for(unsigned i = 0; i < sch->nb_enc; i++) {
        SchEnc* enc = &sch->enc[i];

        err = task_stop(&enc->task);
        ret = err_merge(ret, err);
    }

    for(unsigned i = 0; i < sch->nb_mux; i++) {
        SchMux* mux = &sch->mux[i];

        err = task_stop(&mux->task);
        ret = err_merge(ret, err);
    }

    if(finish_ts)
        *finish_ts = trailing_dts(sch, 1);

    return ret;
}

int sch_wait(Scheduler* sch, uint64_t timeout_us, int64_t* transcode_ts)
{
    int ret, err;
    // convert delay to absolute timestamp
    //timeout_us += av_gettime();

    std::unique_lock<std::mutex> lock(sch->mux_done_lock);
    if(sch->nb_mux_done < sch->nb_mux) {
        auto timeout = std::chrono::system_clock::now() + std::chrono::microseconds(timeout_us);
        sch->mux_done_cond.wait_until(lock, timeout);
    }

    ret = sch->nb_mux_done == sch->nb_mux;

    *transcode_ts = sch->last_dts.load();
    // abort transcoding if any task failed
    err = sch->task_failed.load();
    return ret || err;
}

int sch_filter_command(Scheduler* sch, unsigned fg_idx, AVFrame* frame)
{
    SchFilterGraph* fg;

    av_assert0(fg_idx < sch->nb_filters);
    fg = &sch->filters[fg_idx];

    return send_to_filter(sch, fg, fg->nb_inputs, frame);
}