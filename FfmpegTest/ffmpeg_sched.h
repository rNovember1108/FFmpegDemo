#pragma once



#include "thread_queue.h"
#include "sync_queue.h"
#include <stddef.h>
#include <stdint.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

extern "C"
{
#include "libavcodec/packet.h"
#include "libavutil/avassert.h"
#include "libavutil/error.h"
#include "libavutil/fifo.h"
#include "libavutil/frame.h"
#include "libavutil/mem.h"
#include "libavutil/threadmessage.h"
#include "libavutil/time.h"
}

struct AVFrame;
struct AVPacket;

typedef struct Scheduler Scheduler;

enum SchedulerNodeType {
    SCH_NODE_TYPE_NONE = 0,
    SCH_NODE_TYPE_DEMUX,
    SCH_NODE_TYPE_MUX,
    SCH_NODE_TYPE_DEC,
    SCH_NODE_TYPE_ENC,
    SCH_NODE_TYPE_FILTER_IN,
    SCH_NODE_TYPE_FILTER_OUT,
};

typedef void* (*SchThreadFunc)(void* arg);

#define SCH_DEC(decoder)                                    \
    (SchedulerNode(SCH_NODE_TYPE_DEC, decoder, 0))

#define SCH_ENC(encoder)                                    \
    (SchedulerNode(SCH_NODE_TYPE_ENC, encoder, 0))

#define SCH_FILTER_IN(filter, input)                        \
    (SchedulerNode(SCH_NODE_TYPE_FILTER_IN, filter, input))

#define SCH_DSTREAM(file, stream)                           \
    (SchedulerNode(SCH_NODE_TYPE_DEMUX, file, stream))

#define SCH_FILTER_OUT(filter, output)                      \
    (SchedulerNode(SCH_NODE_TYPE_FILTER_OUT, filter, output))

#define SCH_MSTREAM(file, stream)                           \
    (SchedulerNode(SCH_NODE_TYPE_MUX, file, stream))

enum DemuxSendFlags {
    /**
     * Treat the packet as an EOF for SCH_NODE_TYPE_MUX destinations
     * send normally to other types.
     */
    DEMUX_SEND_STREAMCOPY_EOF = (1 << 0),
};

typedef struct SchedulerNode {
    enum SchedulerNodeType  type;
    unsigned                idx;
    unsigned                idx_stream;
} SchedulerNode;

typedef struct SchDemuxStream {
    SchedulerNode* dst;
    uint8_t* dst_finished;
    unsigned         nb_dst;
} SchDemuxStream;

typedef struct SchTask {
    Scheduler* parent;
    SchedulerNode       node;

    SchThreadFunc       func;
    void* func_arg;

    std::thread           thread;
    int                 thread_running;
} SchTask;

typedef struct SchWaiter {
    std::mutex     lock;
    std::condition_variable      cond;
    std::atomic<int>          choked;

    // the following are internal state of schedule_update_locked() and must not
    // be accessed outside of it
    int                 choked_prev;
    int                 choked_next;
} SchWaiter;

typedef struct SchDemux {
    const AVClass* av_class;

    SchDemuxStream* streams;
    unsigned         nb_streams;

    SchTask             task;
    SchWaiter           waiter;

    // temporary storage used by sch_demux_send()
    AVPacket* send_pkt;

    // protected by schedule_lock
    int                 task_exited;
} SchDemux;

typedef struct PreMuxQueue {
    /**
     * Queue for buffering the packets before the muxer task can be started.
     */
    AVFifo* fifo;
    /**
     * Maximum number of packets in fifo.
     */
    int             max_packets;
    /*
     * The size of the AVPackets' buffers in queue.
     * Updated when a packet is either pushed or pulled from the queue.
     */
    size_t          data_size;
    /* Threshold after which max_packets will be in effect */
    size_t          data_threshold;
} PreMuxQueue;

typedef struct SchMuxStream {
    SchedulerNode       src;
    SchedulerNode       src_sched;

    unsigned* sub_heartbeat_dst;
    unsigned         nb_sub_heartbeat_dst;

    PreMuxQueue         pre_mux_queue;

    // an EOF was generated while flushing the pre-mux queue
    int                 init_eof;

    ////////////////////////////////////////////////////////////
    // The following are protected by Scheduler.schedule_lock //

    /* dts+duration of the last packet sent to this stream
       in AV_TIME_BASE_Q */
    int64_t             last_dts;
    // this stream no longer accepts input
    int                 source_finished;
    ////////////////////////////////////////////////////////////
} SchMuxStream;

typedef struct SchMux {
    const AVClass* av_class;

    SchMuxStream* streams;
    unsigned         nb_streams;
    unsigned         nb_streams_ready;

    int               (*init)(void* arg);

    SchTask             task;
    /**
     * Set to 1 after starting the muxer task and flushing the
     * pre-muxing queues.
     * Set either before any tasks have started, or with
     * Scheduler.mux_ready_lock held.
     */
    std::atomic<int>          mux_started;
    ThreadQueue* queue;

    AVPacket* sub_heartbeat_pkt;
} SchMux;

typedef struct SchDec {
    const AVClass* av_class;

    SchedulerNode       src;
    SchedulerNode* dst;
    uint8_t* dst_finished;
    unsigned         nb_dst;

    SchTask             task;
    // Queue for receiving input packets, one stream.
    ThreadQueue* queue;

    // Queue for sending post-flush end timestamps back to the source
    AVThreadMessageQueue* queue_end_ts;
    int                 expect_end_ts;

    // temporary storage used by sch_dec_send()
    AVFrame* send_frame;
} SchDec;

typedef struct SchEnc {
    const AVClass* av_class;

    SchedulerNode       src;
    SchedulerNode       dst;

    // [0] - index of the sync queue in Scheduler.sq_enc,
    // [1] - index of this encoder in the sq
    int                 sq_idx[2];

    /* Opening encoders is somewhat nontrivial due to their interaction with
     * sync queues, which are (among other things) responsible for maintaining
     * constant audio frame size, when it is required by the encoder.
     *
     * Opening the encoder requires stream parameters, obtained from the first
     * frame. However, that frame cannot be properly chunked by the sync queue
     * without knowing the required frame size, which is only available after
     * opening the encoder.
     *
     * This apparent circular dependency is resolved in the following way:
     * - the caller creating the encoder gives us a callback which opens the
     *   encoder and returns the required frame size (if any)
     * - when the first frame is sent to the encoder, the sending thread
     *      - calls this callback, opening the encoder
     *      - passes the returned frame size to the sync queue
     */
    int               (*open_cb)(void* opaque, const AVFrame* frame);
    int                 opened;

    SchTask             task;
    // Queue for receiving input frames, one stream.
    ThreadQueue* queue;
    // tq_send() to queue returned EOF
    int                 in_finished;
    int                 out_finished;
} SchEnc;

typedef struct SchSyncQueue {
    SyncQueue* sq;
    AVFrame* frame;
    std::mutex     lock;

    unsigned* enc_idx;
    unsigned         nb_enc_idx;
} SchSyncQueue;

typedef struct SchFilterIn {
    SchedulerNode       src;
    SchedulerNode       src_sched;
    int                 send_finished;
} SchFilterIn;

typedef struct SchFilterOut {
    SchedulerNode       dst;
} SchFilterOut;

typedef struct SchFilterGraph {
    const AVClass* av_class;

    SchFilterIn* inputs;
    unsigned         nb_inputs;
    std::atomic<unsigned int>      nb_inputs_finished;

    SchFilterOut* outputs;
    unsigned         nb_outputs;

    SchTask             task;
    // input queue, nb_inputs+1 streams
    // last stream is control
    ThreadQueue* queue;
    SchWaiter           waiter;

    // protected by schedule_lock
    unsigned            best_input;
    int                 task_exited;
} SchFilterGraph;

struct Scheduler {
    const AVClass* av_class;

    SchDemux* demux;
    unsigned         nb_demux;

    SchMux* mux;
    unsigned         nb_mux;

    unsigned         nb_mux_ready;
    std::mutex     mux_ready_lock;

    unsigned         nb_mux_done;
    std::mutex     mux_done_lock;
    std::condition_variable      mux_done_cond;


    SchDec* dec;
    unsigned         nb_dec;

    SchEnc* enc;
    unsigned         nb_enc;

    SchSyncQueue* sq_enc;
    unsigned         nb_sq_enc;

    SchFilterGraph* filters;
    unsigned         nb_filters;

    char* sdp_filename;
    int                 sdp_auto;

    int                 transcode_started;
    std::atomic<int>          terminate;
    std::atomic<int>          task_failed;

    std::mutex     schedule_lock;

    std::atomic<int_least64_t> last_dts;
};

Scheduler* sch_alloc(void);

/**
 * Add a demuxer to the scheduler.
 *
 * @param func Function executed as the demuxer task.
 * @param ctx Demuxer state; will be passed to func and used for logging.
 *
 * @retval ">=0" Index of the newly-created demuxer.
 * @retval "<0"  Error code.
 */
int sch_add_demux(Scheduler* sch, SchThreadFunc func, void* ctx);

int sch_demux_send(Scheduler* sch, unsigned demux_idx, struct AVPacket* pkt,
    unsigned flags);

int sch_connect(Scheduler* sch, SchedulerNode src, SchedulerNode dst);

/**
 * Add a demuxed stream for a previously added demuxer.
 *
 * @param demux_idx index previously returned by sch_add_demux()
 *
 * @retval ">=0" Index of the newly-created demuxed stream.
 * @retval "<0"  Error code.
 */
int sch_add_demux_stream(Scheduler* sch, unsigned demux_idx);

/**
 * Add a decoder to the scheduler.
 *
 * @param func Function executed as the decoder task.
 * @param ctx Decoder state; will be passed to func and used for logging.
 * @param send_end_ts The decoder will return an end timestamp after flush packets
 *                    are delivered to it. See documentation for
 *                    sch_dec_receive() for more details.
 *
 * @retval ">=0" Index of the newly-created decoder.
 * @retval "<0"  Error code.
 */
int sch_add_dec(Scheduler* sch, SchThreadFunc func, void* ctx,
    int send_end_ts);

/**
 * Called by decoder tasks to receive a packet for decoding.
 *
 * @param dec_idx decoder index
 * @param pkt Input packet will be written here on success.
 *
 *            An empty packet signals that the decoder should be flushed, but
 *            more packets will follow (e.g. after seeking). When a decoder
 *            created with send_end_ts=1 receives a flush packet, it must write
 *            the end timestamp of the stream after flushing to
 *            pkt->pts/time_base on the next call to this function (if any).
 *
 * @retval "non-negative value" success
 * @retval AVERROR_EOF no more packets will arrive, should terminate decoding
 * @retval "another negative error code" other failure
 */
int sch_dec_receive(Scheduler* sch, unsigned dec_idx, struct AVPacket* pkt);

/**
 * Called by decoder tasks to send a decoded frame downstream.
 *
 * @param dec_idx Decoder index previously returned by sch_add_dec().
 * @param frame Decoded frame; on success it is consumed and cleared by this
 *              function
 *
 * @retval ">=0" success
 * @retval AVERROR_EOF all consumers are done, should terminate decoding
 * @retval "another negative error code" other failure
 */
int sch_dec_send(Scheduler* sch, unsigned dec_idx, struct AVFrame* frame);

/**
 * Add a muxer to the scheduler.
 *
 * Note that muxer thread startup is more complicated than for other components,
 * because
 * - muxer streams fed by audio/video encoders become initialized dynamically at
 *   runtime, after those encoders receive their first frame and initialize
 *   themselves, followed by calling sch_mux_stream_ready()
 * - the header can be written after all the streams for a muxer are initialized
 * - we may need to write an SDP, which must happen
 *      - AFTER all the headers are written
 *      - BEFORE any packets are written by any muxer
 *      - with all the muxers quiescent
 * To avoid complicated muxer-thread synchronization dances, we postpone
 * starting the muxer threads until after the SDP is written. The sequence of
 * events is then as follows:
 * - After sch_mux_stream_ready() is called for all the streams in a given muxer,
 *   the header for that muxer is written (care is taken that headers for
 *   different muxers are not written concurrently, since they write file
 *   information to stderr). If SDP is not wanted, the muxer thread then starts
 *   and muxing begins.
 * - When SDP _is_ wanted, no muxer threads start until the header for the last
 *   muxer is written. After that, the SDP is written, after which all the muxer
 *   threads are started at once.
 *
 * In order for the above to work, the scheduler needs to be able to invoke
 * just writing the header, which is the reason the init parameter exists.
 *
 * @param func Function executed as the muxing task.
 * @param init Callback that is called to initialize the muxer and write the
 *             header. Called after sch_mux_stream_ready() is called for all the
 *             streams in the muxer.
 * @param ctx Muxer state; will be passed to func/init and used for logging.
 * @param sdp_auto Determines automatic SDP writing - see sch_sdp_filename().
 *
 * @retval ">=0" Index of the newly-created muxer.
 * @retval "<0"  Error code.
 */
int sch_add_mux(Scheduler* sch, SchThreadFunc func, int (*init)(void*),
    void* ctx, int sdp_auto);

/**
 * Called by muxer tasks to obtain packets for muxing. Will wait for a packet
 * for any muxed stream to become available and return it in pkt.
 *
 * @param mux_idx Muxer index previously returned by sch_add_mux().
 * @param pkt     Newly-received packet will be stored here on success. Must be
 *                clean on entrance to this function.
 *
 * @retval 0 A packet was successfully delivered into pkt. Its stream_index
 *           corresponds to a stream index previously returned from
 *           sch_add_mux_stream().
 * @retval AVERROR_EOF When pkt->stream_index is non-negative, this signals that
 *                     no more packets will be delivered for this stream index.
 *                     Otherwise this indicates that no more packets will be
 *                     delivered for any stream and the muxer should therefore
 *                     flush everything and terminate.
 */
int sch_mux_receive(Scheduler* sch, unsigned mux_idx, struct AVPacket* pkt);

int sch_mux_sub_heartbeat(Scheduler* sch, unsigned mux_idx, unsigned stream_idx,
    const AVPacket* pkt);

/**
 * Called by muxer tasks to signal that a stream will no longer accept input.
 *
 * @param stream_idx Stream index previously returned from sch_add_mux_stream().
 */
void sch_mux_receive_finish(Scheduler* sch, unsigned mux_idx, unsigned stream_idx);

/**
 * Add a muxed stream for a previously added muxer.
 *
 * @param mux_idx index previously returned by sch_add_mux()
 *
 * @retval ">=0" Index of the newly-created muxed stream.
 * @retval "<0"  Error code.
 */
int sch_add_mux_stream(Scheduler* sch, unsigned mux_idx);

/**
 * Add an encoder to the scheduler.
 *
 * @param func Function executed as the encoding task.
 * @param ctx Encoder state; will be passed to func and used for logging.
 * @param open_cb This callback, if specified, will be called when the first
 *                frame is obtained for this encoder. For audio encoders with a
 *                fixed frame size (which use a sync queue in the scheduler to
 *                rechunk frames), it must return that frame size on success.
 *                Otherwise (non-audio, variable frame size) it should return 0.
 *
 * @retval ">=0" Index of the newly-created encoder.
 * @retval "<0"  Error code.
 */
int sch_add_enc(Scheduler* sch, SchThreadFunc func, void* ctx,
    int (*open_cb)(void* func_arg, const struct AVFrame* frame));

/**
 * Signal to the scheduler that the specified muxed stream is initialized and
 * ready. Muxing is started once all the streams are ready.
 */
int sch_mux_stream_ready(Scheduler* sch, unsigned mux_idx, unsigned stream_idx);

/**
 * Called by encoder tasks to obtain frames for encoding. Will wait for a frame
 * to become available and return it in frame.
 *
 * @param enc_idx Encoder index previously returned by sch_add_enc().
 * @param frame   Newly-received frame will be stored here on success. Must be
 *                clean on entrance to this function.
 *
 * @retval 0 A frame was successfully delivered into frame.
 * @retval AVERROR_EOF No more frames will be delivered, the encoder should
 *                     flush everything and terminate.
 *
 */
int sch_enc_receive(Scheduler* sch, unsigned enc_idx, struct AVFrame* frame);

/**
 * Called by encoder tasks to send encoded packets downstream.
 *
 * @param enc_idx Encoder index previously returned by sch_add_enc().
 * @param pkt     An encoded packet; it will be consumed and cleared by this
 *                function on success.
 *
 * @retval 0     success
 * @retval "<0"  Error code.
 */
int sch_enc_send(Scheduler* sch, unsigned enc_idx, struct AVPacket* pkt);

int sch_mux_sub_heartbeat_add(Scheduler* sch, unsigned mux_idx, unsigned stream_idx,
    unsigned dec_idx);

/**
 * Add an pre-encoding sync queue to the scheduler.
 *
 * @param buf_size_us Sync queue buffering size, passed to sq_alloc().
 * @param logctx Logging context for the sync queue. passed to sq_alloc().
 *
 * @retval ">=0" Index of the newly-created sync queue.
 * @retval "<0"  Error code.
 */
int sch_add_sq_enc(Scheduler* sch, uint64_t buf_size_us, void* logctx);
int sch_sq_add_enc(Scheduler* sch, unsigned sq_idx, unsigned enc_idx,
    int limiting, uint64_t max_frames);

/**
 * Configure limits on packet buffering performed before the muxer task is
 * started.
 *
 * @param mux_idx index previously returned by sch_add_mux()
 * @param stream_idx_idx index previously returned by sch_add_mux_stream()
 * @param data_threshold Total size of the buffered packets' data after which
 *                       max_packets applies.
 * @param max_packets maximum Maximum number of buffered packets after
 *                            data_threshold is reached.
 */
void sch_mux_stream_buffering(Scheduler* sch, unsigned mux_idx, unsigned stream_idx,
    size_t data_threshold, int max_packets);

/**
 * Add a filtergraph to the scheduler.
 *
 * @param nb_inputs Number of filtergraph inputs.
 * @param nb_outputs number of filtergraph outputs
 * @param func Function executed as the filtering task.
 * @param ctx Filter state; will be passed to func and used for logging.
 *
 * @retval ">=0" Index of the newly-created filtergraph.
 * @retval "<0"  Error code.
 */
int sch_add_filtergraph(Scheduler* sch, unsigned nb_inputs, unsigned nb_outputs,
    SchThreadFunc func, void* ctx);

/**
 * Called by filtergraph tasks to obtain frames for filtering. Will wait for a
 * frame to become available and return it in frame.
 *
 * Filtergraphs that contain lavfi sources and do not currently require new
 * input frames should call this function as a means of rate control - then
 * in_idx should be set equal to nb_inputs on entry to this function.
 *
 * @param fg_idx Filtergraph index previously returned by sch_add_filtergraph().
 * @param[in,out] in_idx On input contains the index of the input on which a frame
 *                       is most desired. May be set to nb_inputs to signal that
 *                       the filtergraph does not need more input currently.
 *
 *                       On success, will be replaced with the input index of
 *                       the actually returned frame or EOF timestamp.
 *
 * @retval ">=0" Frame data or EOF timestamp was delivered into frame, in_idx
 *               contains the index of the input it belongs to.
 * @retval AVERROR(EAGAIN) No frame was returned, the filtergraph should
 *                         resume filtering. May only be returned when
 *                         in_idx=nb_inputs on entry to this function.
 * @retval AVERROR_EOF No more frames will arrive, should terminate filtering.
 */
int sch_filter_receive(Scheduler* sch, unsigned fg_idx,
    unsigned* in_idx, struct AVFrame* frame);

/**
 * Called by filtergraph tasks to send a filtered frame or EOF to consumers.
 *
 * @param fg_idx Filtergraph index previously returned by sch_add_filtergraph().
 * @param out_idx Index of the output which produced the frame.
 * @param frame The frame to send to consumers. When NULL, signals that no more
 *              frames will be produced for the specified output. When non-NULL,
 *              the frame is consumed and cleared by this function on success.
 *
 * @retval "non-negative value" success
 * @retval AVERROR_EOF all consumers are done
 * @retval "anoter negative error code" other failure
 */
int sch_filter_send(Scheduler* sch, unsigned fg_idx, unsigned out_idx,
    struct AVFrame* frame);

int sch_start(Scheduler* sch);
int sch_stop(Scheduler* sch, int64_t* finish_ts);

/**
 * Wait until transcoding terminates or the specified timeout elapses.
 *
 * @param timeout_us Amount of time in microseconds after which this function
 *                   will timeout.
 * @param transcode_ts Current transcode timestamp in AV_TIME_BASE_Q, for
 *                     informational purposes only.
 *
 * @retval 0 waiting timed out, transcoding is not finished
 * @retval 1 transcoding is finished
 */
int sch_wait(Scheduler* sch, uint64_t timeout_us, int64_t* transcode_ts);

int sch_filter_command(Scheduler* sch, unsigned fg_idx, struct AVFrame* frame);