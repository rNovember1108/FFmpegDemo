#pragma once
#include <stdint.h>
#include <stdio.h>
#include <signal.h>
#include "cmdutils.h"
#include "ffmpeg_sched.h"
#include "ffmpeg_utils.h"

extern "C"
{
#include "libavutil/avassert.h"
#include "libavutil/avstring.h"
#include "libavutil/display.h"
#include "libavutil/error.h"
#include "libavutil/intreadwrite.h"
#include "libavutil/opt.h"
#include "libavutil/parseutils.h"
#include "libavutil/pixdesc.h"
#include "libavutil/time.h"
#include "libavutil/timestamp.h"
#include "libavcodec/bsf.h"
#include "libavcodec/packet.h"
#include "libavformat/avformat.h"
#include "libavutil/eval.h"
#include "libavutil/bprint.h"
#include "libavutil/iamf.h"
#include "libavformat/avio.h"
#include "libavfilter/buffersrc.h"
#include "libavfilter/buffersink.h"
}

#pragma warning(disable:4996)
#define FFMPEG_ERROR_RATE_EXCEEDED FFERRTAG('E', 'R', 'E', 'D')
#define ABORT_ON_FLAG_EMPTY_OUTPUT        (1 <<  0)
#define ABORT_ON_FLAG_EMPTY_OUTPUT_STREAM (1 <<  1)

extern const OptionDef options[];
extern const char program_name[];
extern int copy_tb;
extern int exit_on_error;
extern int do_pkt_dump;
extern int do_hex_dump;
extern int stdin_interaction;
extern const AVIOInterruptCB int_cb;
extern int copy_ts;
extern int start_at_zero;
extern int debug_ts;
extern float dts_delta_threshold;
extern float dts_error_threshold;
extern int recast_media;
extern int do_benchmark_all;
extern float max_error_rate;
extern FILE* vstats_file;
extern char* vstats_filename;
extern int vstats_version;
extern int auto_conversion_filters;
extern int copy_unknown_streams;
extern int ignore_unknown_streams;
extern std::atomic<unsigned> nb_output_dumped;
extern char* filter_nbthreads;
extern int filter_complex_nbthreads;
extern float frame_drop_threshold;
extern int do_benchmark;
extern int64_t stats_period;
extern int print_stats;
extern AVIOContext* progress_avio;
extern int abort_on_flags;

struct Encoder {
    // combined size of all the packets received from the encoder
    uint64_t data_size;

    // number of packets received from the encoder
    uint64_t packets_encoded;

    int opened;

    Scheduler* sch;
    unsigned        sch_idx;
};

enum {
    KF_FORCE_SOURCE = 1,
#if FFMPEG_OPT_FORCE_KF_SOURCE_NO_DROP
    KF_FORCE_SOURCE_NO_DROP = 2,
#endif
};

enum VideoSyncMethod {
    VSYNC_AUTO = -1,
    VSYNC_PASSTHROUGH,
    VSYNC_CFR,
    VSYNC_VFR,
    VSYNC_VSCFR,
#if FFMPEG_OPT_VSYNC_DROP
    VSYNC_DROP,
#endif
};

enum FrameOpaque {
    FRAME_OPAQUE_SUB_HEARTBEAT = 1,
    FRAME_OPAQUE_EOF,
    FRAME_OPAQUE_SEND_COMMAND,
};

enum EncTimeBase {
    ENC_TIME_BASE_DEMUX = -1,
    ENC_TIME_BASE_FILTER = -2,
};

/* select an input stream for an output stream */
typedef struct StreamMap {
    int disabled;           /* 1 is this mapping is disabled by a negative map */
    int file_index;
    int stream_index;
    char* linklabel;       /* name of an output link, for mapping lavfi outputs */
} StreamMap;

typedef struct OptionsContext {
    OptionGroup* g;

    /* input/output options */
    int64_t start_time;
    int64_t start_time_eof;
    int seek_timestamp;
    const char* format;

    SpecifierOptList codec_names;
    SpecifierOptList audio_ch_layouts;
    SpecifierOptList audio_channels;
    SpecifierOptList audio_sample_rate;
    SpecifierOptList frame_rates;
    SpecifierOptList max_frame_rates;
    SpecifierOptList frame_sizes;
    SpecifierOptList frame_pix_fmts;

    /* input options */
    int64_t input_ts_offset;
    int loop;
    int rate_emu;
    float readrate;
    double readrate_initial_burst;
    int accurate_seek;
    int thread_queue_size;
    int input_sync_ref;
    int find_stream_info;

    SpecifierOptList ts_scale;
    SpecifierOptList dump_attachment;
    SpecifierOptList hwaccels;
    SpecifierOptList hwaccel_devices;
    SpecifierOptList hwaccel_output_formats;
    SpecifierOptList autorotate;

    /* output options */
    StreamMap* stream_maps;
    int     nb_stream_maps;
#if FFMPEG_OPT_MAP_CHANNEL
    AudioChannelMap* audio_channel_maps; /* one info entry per -map_channel */
    int           nb_audio_channel_maps; /* number of (valid) -map_channel settings */
#endif
    const char** attachments;
    int       nb_attachments;

    int chapters_input_file;

    int64_t recording_time;
    int64_t stop_time;
    int64_t limit_filesize;
    float mux_preload;
    float mux_max_delay;
    float shortest_buf_duration;
    int shortest;
    int bitexact;

    int video_disable;
    int audio_disable;
    int subtitle_disable;
    int data_disable;

    // keys are stream indices
    AVDictionary* streamid;

    SpecifierOptList metadata;
    SpecifierOptList max_frames;
    SpecifierOptList bitstream_filters;
    SpecifierOptList codec_tags;
    SpecifierOptList sample_fmts;
    SpecifierOptList qscale;
    SpecifierOptList forced_key_frames;
    SpecifierOptList fps_mode;
    SpecifierOptList force_fps;
    SpecifierOptList frame_aspect_ratios;
    SpecifierOptList display_rotations;
    SpecifierOptList display_hflips;
    SpecifierOptList display_vflips;
    SpecifierOptList rc_overrides;
    SpecifierOptList intra_matrices;
    SpecifierOptList inter_matrices;
    SpecifierOptList chroma_intra_matrices;
#if FFMPEG_OPT_TOP
    SpecifierOptList top_field_first;
#endif
    SpecifierOptList metadata_map;
    SpecifierOptList presets;
    SpecifierOptList copy_initial_nonkeyframes;
    SpecifierOptList copy_prior_start;
    SpecifierOptList filters;
#if FFMPEG_OPT_FILTER_SCRIPT
    SpecifierOptList filter_scripts;
#endif
    SpecifierOptList reinit_filters;
    SpecifierOptList fix_sub_duration;
    SpecifierOptList fix_sub_duration_heartbeat;
    SpecifierOptList canvas_sizes;
    SpecifierOptList pass;
    SpecifierOptList passlogfiles;
    SpecifierOptList max_muxing_queue_size;
    SpecifierOptList muxing_queue_data_threshold;
    SpecifierOptList guess_layout_max;
    SpecifierOptList apad;
    SpecifierOptList discard;
    SpecifierOptList disposition;
    SpecifierOptList program;
    SpecifierOptList stream_groups;
    SpecifierOptList time_bases;
    SpecifierOptList enc_time_bases;
    SpecifierOptList autoscale;
    SpecifierOptList bits_per_raw_sample;
    SpecifierOptList enc_stats_pre;
    SpecifierOptList enc_stats_post;
    SpecifierOptList mux_stats;
    SpecifierOptList enc_stats_pre_fmt;
    SpecifierOptList enc_stats_post_fmt;
    SpecifierOptList mux_stats_fmt;
} OptionsContext;

typedef struct InputFilter {
    struct FilterGraph* graph;
    uint8_t* name;
} InputFilter;

struct Decoder {
    AVFrame* frame;
    AVPacket* pkt;

    enum AVPixelFormat hwaccel_pix_fmt;

    // pts/estimated duration of the last decoded frame
    // * in decoder timebase for video,
    // * in last_frame_tb (may change during decoding) for audio
    int64_t         last_frame_pts;
    int64_t         last_frame_duration_est;
    AVRational      last_frame_tb;
    int64_t         last_filter_in_rescale_delta;
    int             last_frame_sample_rate;

    /* previous decoded subtitles */
    AVFrame* sub_prev[2];
    AVFrame* sub_heartbeat;

    Scheduler* sch;
    unsigned        sch_idx;
};

typedef struct InputStream {
    const AVClass* av_class;

    /* parent source */
    struct InputFile* file;

    int index;

    AVStream* st;
    int user_set_discard;
    int decoding_needed;     /* non zero if the packets must be decoded in 'raw_fifo', see DECODING_FOR_* */
#define DECODING_FOR_OST    1
#define DECODING_FOR_FILTER 2

    /**
     * Codec parameters - to be used by the decoding/streamcopy code.
     * st->codecpar should not be accessed, because it may be modified
     * concurrently by the demuxing thread.
     */
    AVCodecParameters* par;
    Decoder* decoder;
    AVCodecContext* dec_ctx;
    const AVCodec* dec;

    AVRational framerate_guessed;

    AVDictionary* decoder_opts;
    AVRational framerate;               /* framerate forced with -r */
#if FFMPEG_OPT_TOP
    int top_field_first;
#endif

    int autorotate;

    int fix_sub_duration;

    struct sub2video {
        int w, h;
    } sub2video;

    /* decoded data from this stream goes into all those filters
     * currently video and audio only */
    InputFilter** filters;
    int        nb_filters;

    /*
     * Output targets that do not go through lavfi, i.e. subtitles or
     * streamcopy. Those two cases are distinguished by the OutputStream
     * having an encoder or not.
     */
    struct OutputStream** outputs;
    int                nb_outputs;

    int reinit_filters;

    /* hwaccel options */
    enum HWAccelID hwaccel_id;
    enum AVHWDeviceType hwaccel_device_type;
    char* hwaccel_device;
    enum AVPixelFormat hwaccel_output_format;

    /* stats */
    // number of frames/samples retrieved from the decoder
    uint64_t frames_decoded;
    uint64_t samples_decoded;
    uint64_t decode_errors;
} InputStream;

typedef struct InputFile {
    const AVClass* av_class;

    int index;

    // input format has no timestamps
    int format_nots;

    AVFormatContext* ctx;
    int64_t input_ts_offset;
    int input_sync_ref;
    /**
     * Effective format start time based on enabled streams.
     */
    int64_t start_time_effective;
    int64_t ts_offset;
    int64_t start_time;   /* user-specified start time in AV_TIME_BASE or AV_NOPTS_VALUE */
    int64_t recording_time;

    /* streams that ffmpeg is aware of;
     * there may be extra streams in ctx that are not mapped to an InputStream
     * if new streams appear dynamically during demuxing */
    InputStream** streams;
    int        nb_streams;

    int accurate_seek;
} InputFile;

extern InputFile** input_files;
extern int  nb_input_files;

typedef struct Demuxer {
    InputFile f;

    // name used for logging
    char log_name[32];

    int64_t wallclock_start;

    /**
     * Extra timestamp offset added by discontinuity handling.
     */
    int64_t ts_offset_discont;
    int64_t last_ts;

    /* number of times input stream should be looped */
    int loop;
    int have_audio_dec;
    /* duration of the looped segment of the input file */
    Timestamp duration;
    /* pts with the smallest/largest values ever seen */
    Timestamp min_pts;
    Timestamp max_pts;

    /* number of streams that the user was warned of */
    int nb_streams_warn;

    float  readrate;
    double readrate_initial_burst;

    Scheduler* sch;

    AVPacket* pkt_heartbeat;

    int                   read_started;
    int                   nb_streams_used;
    int                   nb_streams_finished;
} Demuxer;

typedef struct DemuxThreadContext {
    // packet used for reading from the demuxer
    AVPacket* pkt_demux;
    // packet for reading from BSFs
    AVPacket* pkt_bsf;
} DemuxThreadContext;

typedef struct DemuxStream {
    InputStream ist;

    // name used for logging
    char log_name[32];

    int sch_idx_stream;
    int sch_idx_dec;

    double ts_scale;

    /* true if stream data should be discarded */
    int discard;

    // scheduler returned EOF for this stream
    int finished;

    int streamcopy_needed;
    int have_sub2video;

    int wrap_correction_done;
    int saw_first_ts;
    ///< dts of the first packet read for this stream (in AV_TIME_BASE units)
    int64_t first_dts;

    /* predicted dts of the next packet read for this stream or (when there are
     * several frames in a packet) of the next frame in current packet (in AV_TIME_BASE units) */
    int64_t       next_dts;
    ///< dts of the last packet read for this stream (in AV_TIME_BASE units)
    int64_t       dts;

    const AVCodecDescriptor* codec_desc;

    AVBSFContext* bsf;

    /* number of packets successfully read for this stream */
    uint64_t nb_packets;
    // combined size of all the packets read
    uint64_t data_size;
} DemuxStream;


typedef struct OutputFile {
    const AVClass* av_class;

    int index;

    const AVOutputFormat* format;
    const char* url;

    OutputStream** streams;
    int         nb_streams;

    int64_t recording_time;  ///< desired length of the resulting file in microseconds == AV_TIME_BASE units
    int64_t start_time;      ///< start time in microseconds == AV_TIME_BASE units

    int shortest;
    int bitexact;
} OutputFile;

extern OutputFile** output_files;
extern int         nb_output_files;

typedef struct Muxer {
    OutputFile of;

    // name used for logging
    char log_name[32];

    AVFormatContext* fc;

    Scheduler* sch;
    unsigned     sch_idx;

    // OutputStream indices indexed by scheduler stream indices
    int* sch_stream_idx;
    int       nb_sch_stream_idx;

    AVDictionary* opts;

    int thread_queue_size;

    /* filesize limit expressed in bytes */
    int64_t limit_filesize;
    std::atomic<int_least64_t> last_filesize;
    int header_written;

    SyncQueue* sq_mux;
    AVPacket* sq_pkt;
} Muxer;

enum LatencyProbe {
    LATENCY_PROBE_DEMUX,
    LATENCY_PROBE_DEC_PRE,
    LATENCY_PROBE_DEC_POST,
    LATENCY_PROBE_FILTER_PRE,
    LATENCY_PROBE_FILTER_POST,
    LATENCY_PROBE_ENC_PRE,
    LATENCY_PROBE_ENC_POST,
    LATENCY_PROBE_NB,
};

// optionally attached as opaque_ref to decoded AVFrames
typedef struct FrameData {
    // demuxer-estimated dts in AV_TIME_BASE_Q,
    // to be used when real dts is missing
    int64_t dts_est;

    // properties that come from the decoder
    struct {
        uint64_t   frame_num;

        int64_t    pts;
        AVRational tb;
    } dec;

    AVRational frame_rate_filter;

    int        bits_per_raw_sample;

    int64_t wallclock[LATENCY_PROBE_NB];
} FrameData;

typedef struct OutputFilter {
    struct OutputStream* ost;
    struct FilterGraph* graph;
    uint8_t* name;

    /* for filters that are not yet bound to an output stream,
     * this stores the output linklabel, if any */
    uint8_t* linklabel;

    enum AVMediaType     type;

    std::atomic<uint_least64_t> nb_frames_dup;
    std::atomic<uint_least64_t> nb_frames_drop;
} OutputFilter;

typedef struct FilterGraph {
    const AVClass* av_class;
    int            index;

    InputFilter** inputs;
    int          nb_inputs;
    OutputFilter** outputs;
    int         nb_outputs;
} FilterGraph;

extern FilterGraph** filtergraphs;
extern int        nb_filtergraphs;

enum HWAccelID {
    HWACCEL_NONE = 0,
    HWACCEL_AUTO,
    HWACCEL_GENERIC,
};

enum PacketOpaque {
    PKT_OPAQUE_SUB_HEARTBEAT = 1,
    PKT_OPAQUE_FIX_SUB_DURATION,
};

typedef struct HWDevice {
    const char* name;
    enum AVHWDeviceType type;
    AVBufferRef* device_ref;
} HWDevice;

extern HWDevice* filter_hw_device;

enum forced_keyframes_const {
    FKF_N,
    FKF_N_FORCED,
    FKF_PREV_FORCED_N,
    FKF_PREV_FORCED_T,
    FKF_T,
    FKF_NB
};

typedef struct KeyframeForceCtx {
    int          type;

    int64_t      ref_pts;

    // timestamps of the forced keyframes, in AV_TIME_BASE_Q
    int64_t* pts;
    int       nb_pts;
    int          index;

    AVExpr* pexpr;
    double       expr_const_values[FKF_NB];

    int          dropped_keyframe;
} KeyframeForceCtx;

enum EncStatsType {
    ENC_STATS_LITERAL = 0,
    ENC_STATS_FILE_IDX,
    ENC_STATS_STREAM_IDX,
    ENC_STATS_FRAME_NUM,
    ENC_STATS_FRAME_NUM_IN,
    ENC_STATS_TIMEBASE,
    ENC_STATS_TIMEBASE_IN,
    ENC_STATS_PTS,
    ENC_STATS_PTS_TIME,
    ENC_STATS_PTS_IN,
    ENC_STATS_PTS_TIME_IN,
    ENC_STATS_DTS,
    ENC_STATS_DTS_TIME,
    ENC_STATS_SAMPLE_NUM,
    ENC_STATS_NB_SAMPLES,
    ENC_STATS_PKT_SIZE,
    ENC_STATS_BITRATE,
    ENC_STATS_AVG_BITRATE,
    ENC_STATS_KEYFRAME,
};

typedef struct EncStatsComponent {
    enum EncStatsType type;

    uint8_t* str;
    size_t   str_len;
} EncStatsComponent;

typedef struct EncStats {
    EncStatsComponent* components;
    int              nb_components;

    AVIOContext* io;

    std::mutex     lock;
    int                 lock_initialized;
} EncStats;

typedef struct Encoder Encoder;

typedef struct OutputStream {
    const AVClass* av_class;

    enum AVMediaType type;

    /* parent muxer */
    struct OutputFile* file;

    int index;               /* stream index in the output file */

    /**
     * Codec parameters for packets submitted to the muxer (i.e. before
     * bitstream filtering, if any).
     */
    AVCodecParameters* par_in;

    /* input stream that is the source for this output stream;
     * may be NULL for streams with no well-defined source, e.g.
     * attachments or outputs from complex filtergraphs */
    InputStream* ist;

    AVStream* st;            /* stream in the output file */

    AVRational enc_timebase;

    Encoder* enc;
    AVCodecContext* enc_ctx;

    /* video only */
    AVRational frame_rate;
    AVRational max_frame_rate;
    enum VideoSyncMethod vsync_method;
    int is_cfr;
    int force_fps;
#if FFMPEG_OPT_TOP
    int top_field_first;
#endif
#if FFMPEG_ROTATION_METADATA
    int rotate_overridden;
#endif
    int autoscale;
    int bitexact;
    int bits_per_raw_sample;
#if FFMPEG_ROTATION_METADATA
    double rotate_override_value;
#endif

    AVRational frame_aspect_ratio;

    KeyframeForceCtx kf;

    /* audio only */
#if FFMPEG_OPT_MAP_CHANNEL
    int* audio_channels_map;             /* list of the channels id to pick from the source stream */
    int audio_channels_mapped;           /* number of channels in audio_channels_map */
#endif

    char* logfile_prefix;
    FILE* logfile;

    OutputFilter* filter;

    AVDictionary* encoder_opts;
    AVDictionary* sws_dict;
    AVDictionary* swr_opts;
    char* apad;

    const char* attachment_filename;

    int keep_pix_fmt;

    /* stats */
    // number of packets send to the muxer
    std::atomic<uint_least64_t> packets_written;
    // number of frames/samples sent to the encoder
    uint64_t frames_encoded;
    uint64_t samples_encoded;

    /* packet quality factor */
    std::atomic<int> quality;

    EncStats enc_stats_pre;
    EncStats enc_stats_post;

    /*
     * bool on whether this stream should be utilized for splitting
     * subtitles utilizing fix_sub_duration at random access points.
     */
    unsigned int fix_sub_duration_heartbeat;
} OutputStream;

typedef struct MuxStream {
    OutputStream ost;

    // name used for logging
    char log_name[32];

    AVBSFContext* bsf_ctx;
    AVPacket* bsf_pkt;

    AVPacket* pkt;

    EncStats stats;

    int sch_idx;
    int sch_idx_enc;
    int sch_idx_src;

    int sq_idx_mux;

    int64_t max_frames;

    // timestamp from which the streamcopied streams should start,
    // in AV_TIME_BASE_Q;
    // everything before it should be discarded
    int64_t ts_copy_start;

    /* dts of the last packet sent to the muxer, in the stream timebase
     * used for making up missing dts values */
    int64_t last_mux_dts;

    int64_t    stream_duration;
    AVRational stream_duration_tb;

    // state for av_rescale_delta() call for audio in write_packet()
    int64_t ts_rescale_delta_last;

    // combined size of all the packets sent to the muxer
    uint64_t data_size_mux;

    int copy_initial_nonkeyframes;
    int copy_prior_start;
    int streamcopy_started;
} MuxStream;

#define SPECIFIER_OPT_FMT_str  "%s"
#define SPECIFIER_OPT_FMT_i    "%i"
#define SPECIFIER_OPT_FMT_i64  "%"PRId64
#define SPECIFIER_OPT_FMT_ui64 "%"PRIu64
#define SPECIFIER_OPT_FMT_f    "%f"
#define SPECIFIER_OPT_FMT_dbl  "%lf"

#define WARN_MULTIPLE_OPT_USAGE(optname, type, idx, st)\
{\
    char namestr[128] = "";\
    const SpecifierOpt *so = &o->optname.opt[idx];\
    const char *spec = so->specifier && so->specifier[0] ? so->specifier : "";\
    snprintf(namestr, sizeof(namestr), "-%s", o->optname.opt_canon->name);\
    if (o->optname.opt_canon->flags & OPT_HAS_ALT) {\
        const char * const *names_alt = o->optname.opt_canon->u1.names_alt;\
        for (int _i = 0; names_alt[_i]; _i++)\
            av_strlcatf(namestr, sizeof(namestr), "/-%s", names_alt[_i]);\
    }\
    av_log(NULL, AV_LOG_WARNING, "Multiple %s options specified for stream %d, only the last option '-%s%s%s "SPECIFIER_OPT_FMT_##type"' will be used.\n",\
           namestr, st->index, o->optname.opt_canon->name, spec[0] ? ":" : "", spec, so->u.type);\
}

#define MATCH_PER_STREAM_OPT(name, type, outvar, fmtctx, st)\
{\
    int _ret, _matches = 0, _match_idx;\
    for (int _i = 0; _i < o->name.nb_opt; _i++) {\
        char *spec = o->name.opt[_i].specifier;\
        if ((_ret = check_stream_specifier(fmtctx, st, spec)) > 0) {\
            outvar = (decltype(outvar))o->name.opt[_i].u.type;\
            _match_idx = _i;\
            _matches++;\
        } else if (_ret < 0)\
            return _ret;\
    }\
    if (_matches > 1 && o->name.opt_canon)\
       WARN_MULTIPLE_OPT_USAGE(name, type, _match_idx, st);\
}

int ffmpeg_parse_options(int argc, char** argv, Scheduler* sch);

int ifile_open(const OptionsContext* o, const char* filename, Scheduler* sch);
int of_open(const OptionsContext* o, const char* filename, Scheduler* sch);

int check_filter_outputs(void);

const char* opt_match_per_type_str(const SpecifierOptList* sol,
    char mediatype);

int find_codec(void* logctx, const char* name,
    enum AVMediaType type, int encoder, const AVCodec** codec);

void remove_avoptions(AVDictionary** a, AVDictionary* b);
int check_avoptions(AVDictionary* m);

AVDictionary* strip_specifiers(const AVDictionary* dict);

int assert_file_overwrite(const char* filename);

FrameData* packet_data(AVPacket* pkt);

void term_init(void);
void term_exit(void);

int init_complex_filtergraph(FilterGraph* fg);

/**
 * Find an unused input stream of given type.
 */
InputStream* ist_find_unused(enum AVMediaType type);

/* iterate over all input streams in all input files;
 * pass NULL to start iteration */
InputStream* ist_iter(InputStream* prev);

int ist_filter_add(InputStream* ist, InputFilter* ifilter, int is_simple);

/**
 * Set up fallback filtering parameters from a decoder context. They will only
 * be used if no frames are ever sent on this input, otherwise the actual
 * parameters are taken from the frame.
 */
int ifilter_parameters_from_dec(InputFilter* ifilter, const AVCodecContext* dec);

int filtergraph_is_simple(const FilterGraph* fg);

void* decoder_thread(void* arg);
int dec_open(InputStream* ist, Scheduler* sch, unsigned sch_idx);
void dec_free(Decoder** pdec);

HWDevice* hw_device_get_by_name(const char* name);
HWDevice* hw_device_get_by_type(enum AVHWDeviceType type);
int hw_device_init_from_type(enum AVHWDeviceType type,
    const char* device,
    HWDevice** dev_out);
int hwaccel_retrieve_data(AVCodecContext* avctx, AVFrame* input);

int subtitle_wrap_frame(AVFrame* frame, AVSubtitle* subtitle, int copy);
int copy_av_subtitle(AVSubtitle* dst, const AVSubtitle* src);

void update_benchmark(const char* fmt, ...);

/**
 * Get our axiliary frame data attached to the frame, allocating it
 * if needed.
 */
FrameData* frame_data(AVFrame* frame);

void* muxer_thread(void* arg);

void enc_stats_write(OutputStream* ost, EncStats* es,
    const AVFrame* frame, const AVPacket* pkt,
    uint64_t frame_num);

int mux_check_init(void* arg);

void* encoder_thread(void* arg);
int enc_open(void* opaque, const AVFrame* frame);

/*
 * Initialize muxing state for the given stream, should be called
 * after the codec/streamcopy setup has been done.
 *
 * Open the muxer once all the streams have been initialized.
 */
int of_stream_init(OutputFile* of, OutputStream* ost);

int print_sdp(const char* filename);

int enc_alloc(Encoder** penc, const AVCodec* codec,
    Scheduler* sch, unsigned sch_idx);
void enc_free(Encoder** penc);

int parse_and_set_vsync(const char* arg, int* vsync_var, int file_idx, int st_idx, int is_global);

int ofilter_bind_ost(OutputFilter* ofilter, OutputStream* ost,
    unsigned sched_idx_enc);

int init_simple_filtergraph(InputStream* ist, OutputStream* ost,
    char* graph_desc,
    Scheduler* sch, unsigned sch_idx_enc);

/**
 * Create a new filtergraph in the global filtergraph list.
 *
 * @param graph_desc Graph description; an av_malloc()ed string, filtergraph
 *                   takes ownership of it.
 */
int fg_create(FilterGraph** pfg, char* graph_desc, Scheduler* sch);

void fg_free(FilterGraph** pfg);

/**
 * Get a hardware device to be used with this filtergraph.
 * The returned reference is owned by the callee, the caller
 * must ref it explicitly for long-term use.
 */
AVBufferRef* hw_device_for_filter(void);

const FrameData* frame_data_c(AVFrame* frame);

int ist_output_add(InputStream* ist, OutputStream* ost);

void show_usage(void);

/* iterate over all output streams in all output files;
 * pass NULL to start iteration */
OutputStream* ost_iter(OutputStream* prev);

void fg_send_command(FilterGraph* fg, double time, const char* target,
    const char* command, const char* arg, int all_filters);

int64_t of_filesize(OutputFile* of);

int of_write_trailer(OutputFile* of);