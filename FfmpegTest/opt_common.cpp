#include "opt_common.h"

extern "C"
{
#include "libavutil/avassert.h"
#include "libavutil/avstring.h"
#include "libavutil/bprint.h"
#include "libavutil/channel_layout.h"
#include "libavutil/cpu.h"
#include "libavutil/dict.h"
#include "libavutil/error.h"
#include "libavutil/ffversion.h"
#include "libavutil/log.h"
#include "libavutil/mem.h"
#include "libavutil/parseutils.h"
#include "libavutil/pixdesc.h"
#include "libavutil/version.h"

#include "libavcodec/avcodec.h"
#include "libavcodec/bsf.h"
#include "libavcodec/codec.h"
#include "libavcodec/codec_desc.h"
#include "libavcodec/version.h"

#include "libavformat/avformat.h"
#include "libavformat/version.h"

#include "libavdevice/avdevice.h"
#include "libavdevice/version.h"

#include "libavfilter/avfilter.h"
#include "libavfilter/version.h"

#include "libswscale/swscale.h"
#include "libswscale/version.h"

#include "libswresample/swresample.h"
#include "libswresample/version.h"

#include "libpostproc/postprocess.h"
#include "libpostproc/version.h"
}

#include "cmdutils.h"
#include "config.h"

#pragma warning(disable:4996)
static FILE* report_file;
static int report_file_level = AV_LOG_DEBUG;
const char program_name[] = "ffmpeg";
const int program_birth_year = 2000;
static int warned_cfg = 0;

#define INDENT        1
#define SHOW_VERSION  2
#define SHOW_CONFIG   4
#define SHOW_COPYRIGHT 8

#define PRINT_LIB_INFO(libname, LIBNAME, flags, level)                  \
    if (CONFIG_##LIBNAME) {                                             \
        const char *indent = flags & INDENT? "  " : "";                 \
        if (flags & SHOW_VERSION) {                                     \
            unsigned int version = libname##_version();                 \
            av_log(NULL, level,                                         \
                   "%slib%-11s %2d.%3d.%3d / %2d.%3d.%3d\n",            \
                   indent, #libname,                                    \
                   LIB##LIBNAME##_VERSION_MAJOR,                        \
                   LIB##LIBNAME##_VERSION_MINOR,                        \
                   LIB##LIBNAME##_VERSION_MICRO,                        \
                   AV_VERSION_MAJOR(version), AV_VERSION_MINOR(version),\
                   AV_VERSION_MICRO(version));                          \
        }                                                               \
        if (flags & SHOW_CONFIG) {                                      \
            const char *cfg = libname##_configuration();                \
            if (strcmp(FFMPEG_CONFIGURATION, cfg)) {                    \
                if (!warned_cfg) {                                      \
                    av_log(NULL, level,                                 \
                            "%sWARNING: library configuration mismatch\n", \
                            indent);                                    \
                    warned_cfg = 1;                                     \
                }                                                       \
                av_log(NULL, level, "%s%-11s configuration: %s\n",      \
                        indent, #libname, cfg);                         \
            }                                                           \
        }                                                               \
    }                                                                   \


#if CONFIG_AVDEVICE
static int show_sinks_sources_parse_arg(const char* arg, char** dev, AVDictionary** opts)
{
    return 0;
}

static void print_device_list(const AVDeviceInfoList* device_list)
{
}

static int print_device_sinks(const AVOutputFormat* fmt, AVDictionary* opts)
{
    return 0;
}

int show_sinks(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

static int print_device_sources(const AVInputFormat* fmt, AVDictionary* opts)
{
    return 0;
}

int show_sources(void* optctx, const char* opt, const char* arg)
{
    return 0;
}
#endif

int show_license(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_help(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_version(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_buildconf(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_formats(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_muxers(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_demuxers(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_devices(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_codecs(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_decoders(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_encoders(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_bsfs(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_protocols(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_filters(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_pix_fmts(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_layouts(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_sample_fmts(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_dispositions(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int show_colors(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int opt_loglevel(void* optctx, const char* opt, const char* arg)
{
    const struct { const char* name; int level; } log_levels[] = {
        { "quiet"  , AV_LOG_QUIET   },
        { "panic"  , AV_LOG_PANIC   },
        { "fatal"  , AV_LOG_FATAL   },
        { "error"  , AV_LOG_ERROR   },
        { "warning", AV_LOG_WARNING },
        { "info"   , AV_LOG_INFO    },
        { "verbose", AV_LOG_VERBOSE },
        { "debug"  , AV_LOG_DEBUG   },
        { "trace"  , AV_LOG_TRACE   },
    };
    const char* token;
    char* tail;
    int flags = av_log_get_flags();
    int level = av_log_get_level();
    int cmd, i = 0;

    av_assert0(arg);
    while(*arg) {
        token = arg;
        if(*token == '+' || *token == '-') {
            cmd = *token++;
        }
        else {
            cmd = 0;
        }
        if(!i && !cmd) {
            flags = 0;  /* missing relative prefix, build absolute value */
        }
        if(av_strstart(token, "repeat", &arg)) {
            if(cmd == '-') {
                flags |= AV_LOG_SKIP_REPEATED;
            }
            else {
                flags &= ~AV_LOG_SKIP_REPEATED;
            }
        }
        else if(av_strstart(token, "level", &arg)) {
            if(cmd == '-') {
                flags &= ~AV_LOG_PRINT_LEVEL;
            }
            else {
                flags |= AV_LOG_PRINT_LEVEL;
            }
        }
        else {
            break;
        }
        i++;
    }
    if(!*arg) {
        goto end;
    }
    else if(*arg == '+') {
        arg++;
    }
    else if(!i) {
        flags = av_log_get_flags();  /* level value without prefix, reset flags */
    }

    for(i = 0; i < FF_ARRAY_ELEMS(log_levels); i++) {
        if(!strcmp(log_levels[i].name, arg)) {
            level = log_levels[i].level;
            goto end;
        }
    }

    level = strtol(arg, &tail, 10);
    if(*tail) {
        av_log(NULL, AV_LOG_FATAL, "Invalid loglevel \"%s\". "
            "Possible levels are numbers or:\n", arg);
        for(i = 0; i < FF_ARRAY_ELEMS(log_levels); i++)
            av_log(NULL, AV_LOG_FATAL, "\"%s\"\n", log_levels[i].name);
        return AVERROR(EINVAL);
    }

end:
    av_log_set_flags(flags);
    av_log_set_level(level);
    return 0;
}

int opt_report(void* optctx, const char* opt, const char* arg)
{
    return init_report(NULL, NULL);
}

static void expand_filename_template(AVBPrint* bp, const char* template1,
    struct tm* tm)
{
    int c;

    while((c = *(template1++))) {
        if(c == '%') {
            if(!(c = *(template1++)))
                break;
            switch(c) {
            case 'p':
                av_bprintf(bp, "%s", program_name);
                break;
            case 't':
                av_bprintf(bp, "%04d%02d%02d-%02d%02d%02d",
                    tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
                    tm->tm_hour, tm->tm_min, tm->tm_sec);
                break;
            case '%':
                av_bprint_chars(bp, c, 1);
                break;
            }
        }
        else {
            av_bprint_chars(bp, c, 1);
        }
    }
}

static void log_callback_report(void* ptr, int level, const char* fmt, va_list vl)
{
    va_list vl2;
    char line[1024];
    static int print_prefix = 1;

    va_copy(vl2, vl);
    av_log_default_callback(ptr, level, fmt, vl);
    av_log_format_line(ptr, level, fmt, vl2, line, sizeof(line), &print_prefix);
    va_end(vl2);
    if(report_file_level >= level) {
        fputs(line, report_file);
        fflush(report_file);
    }
}

int init_report(const char* env, FILE** file)
{
    char* filename_template = NULL;
    char* key, * val;
    int ret, count = 0;
    int prog_loglevel, envlevel = 0;
    time_t now;
    struct tm* tm;
    AVBPrint filename;

    if(report_file) /* already opened */
        return 0;
    time(&now);
    tm = localtime(&now);

    while(env && *env) {
        if((ret = av_opt_get_key_value(&env, "=", ":", 0, &key, &val)) < 0) {
            if(count)
                av_log(NULL, AV_LOG_ERROR,
                    "Failed to parse FFREPORT environment variable: %s\n",
                    av_err2str(ret));
            break;
        }
        if(*env)
            env++;
        count++;
        if(!strcmp(key, "file")) {
            av_free(filename_template);
            filename_template = val;
            val = NULL;
        }
        else if(!strcmp(key, "level")) {
            char* tail;
            report_file_level = strtol(val, &tail, 10);
            if(*tail) {
                av_log(NULL, AV_LOG_FATAL, "Invalid report file level\n");
                av_free(key);
                av_free(val);
                av_free(filename_template);
                return AVERROR(EINVAL);
            }
            envlevel = 1;
        }
        else {
            av_log(NULL, AV_LOG_ERROR, "Unknown key '%s' in FFREPORT\n", key);
        }
        av_free(val);
        av_free(key);
    }

    av_bprint_init(&filename, 0, AV_BPRINT_SIZE_AUTOMATIC);
    expand_filename_template(&filename,
        (const char*)av_x_if_null(filename_template, "%p-%t.log"), tm);
    av_free(filename_template);
    if(!av_bprint_is_complete(&filename)) {
        av_log(NULL, AV_LOG_ERROR, "Out of memory building report file name\n");
        return AVERROR(ENOMEM);
    }

    prog_loglevel = av_log_get_level();
    if(!envlevel)
        report_file_level = FFMAX(report_file_level, prog_loglevel);

    report_file = fopen(filename.str, "w");
    if(!report_file) {
        int ret = AVERROR(errno);
        av_log(NULL, AV_LOG_ERROR, "Failed to open report \"%s\": %s\n",
            filename.str, strerror(errno));
        return ret;
    }
    av_log_set_callback(log_callback_report);
    av_log(NULL, AV_LOG_INFO,
        "%s started on %04d-%02d-%02d at %02d:%02d:%02d\n"
        "Report written to \"%s\"\n"
        "Log level: %d\n",
        program_name,
        tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
        tm->tm_hour, tm->tm_min, tm->tm_sec,
        filename.str, report_file_level);
    av_bprint_finalize(&filename, NULL);

    if(file)
        *file = report_file;

    return 0;
}

int opt_max_alloc(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int opt_cpuflags(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

int opt_cpucount(void* optctx, const char* opt, const char* arg)
{
    return 0;
}

static void print_program_info(int flags, int level)
{
    const char* indent = flags & INDENT ? "  " : "";

    av_log(NULL, level, "%s version " FFMPEG_VERSION, program_name);
    if(flags & SHOW_COPYRIGHT)
        av_log(NULL, level, " Copyright (c) %d-%d the FFmpeg developers",
            program_birth_year, CONFIG_THIS_YEAR);
    av_log(NULL, level, "\n");
    // av_log(NULL, level, "%sbuilt with %s\n", indent, CC_IDENT);

    av_log(NULL, level, "%sconfiguration: " FFMPEG_CONFIGURATION "\n", indent);
}

static void print_all_libs_info(int flags, int level)
{
    PRINT_LIB_INFO(avutil, AVUTIL, flags, level);
    PRINT_LIB_INFO(avcodec, AVCODEC, flags, level);
    PRINT_LIB_INFO(avformat, AVFORMAT, flags, level);
    PRINT_LIB_INFO(avdevice, AVDEVICE, flags, level);
    PRINT_LIB_INFO(avfilter, AVFILTER, flags, level);
    PRINT_LIB_INFO(swscale, SWSCALE, flags, level);
    PRINT_LIB_INFO(swresample, SWRESAMPLE, flags, level);
    PRINT_LIB_INFO(postproc, POSTPROC, flags, level);
}

void show_banner(int argc, char** argv, const OptionDef* options)
{
    int idx = locate_option(argc, argv, options, "version");
    if(hide_banner || idx)
        return;

    print_program_info(INDENT | SHOW_COPYRIGHT, AV_LOG_INFO);
    print_all_libs_info(INDENT | SHOW_CONFIG, AV_LOG_INFO);
    print_all_libs_info(INDENT | SHOW_VERSION, AV_LOG_INFO);
}