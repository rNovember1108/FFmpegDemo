#include "cmdutils.h"

extern "C"
{
#include "libavformat/avformat.h"
#include "libswscale/swscale.h"
#include "libswscale/version.h"
#include "libswresample/swresample.h"
#include "libavutil/avassert.h"
#include "libavutil/avstring.h"
#include "libavutil/bprint.h"
#include "libavutil/channel_layout.h"
#include "libavutil/display.h"
#include "libavutil/mathematics.h"
#include "libavutil/imgutils.h"
#include "libavutil/parseutils.h"
#include "libavutil/eval.h"
#include "libavutil/dict.h"
#include "libavutil/opt.h"
}

#include "opt_common.h"
#pragma warning(disable:4996)

int hide_banner = 0;
AVDictionary* sws_dict;
AVDictionary* swr_opts;
AVDictionary* format_opts, * codec_opts;

void log_callback_help(void* ptr, int level, const char* fmt, va_list vl)
{
    vfprintf(stdout, fmt, vl);
}

static void check_options(const OptionDef* po)
{
    while(po->name)
    {
        if(po->flags & OPT_PERFILE)
            av_assert0(po->flags & (OPT_INPUT | OPT_OUTPUT));

        if(po->type == OPT_TYPE_FUNC)
            av_assert0(!(po->flags & (OPT_FLAG_OFFSET | OPT_FLAG_SPEC)));

        // OPT_FUNC_ARG can only be ser for OPT_TYPE_FUNC
        av_assert0((po->type == OPT_TYPE_FUNC) || !(po->flags & OPT_FUNC_ARG));

        po++;
    }
}

static void dump_argument(FILE* report_file, const char* a)
{
    const char* p;

    for(p = a; *p; p++)
        if(!((*p >= '+' && *p <= ':') || (*p >= '@' && *p <= 'Z') ||
            *p == '_' || (*p >= 'a' && *p <= 'z')))
            break;
    if(!*p) {
        fputs(a, report_file);
        return;
    }
    fputc('"', report_file);
    for(p = a; *p; p++) {
        if(*p == '\\' || *p == '"' || *p == '$' || *p == '`')
            fprintf(report_file, "\\%c", *p);
        else if(*p < ' ' || *p > '~')
            fprintf(report_file, "\\x%02x", *p);
        else
            fputc(*p, report_file);
    }
    fputc('"', report_file);
}

void parse_loglevel(int argc, char** argv, const OptionDef* options)
{
    // 一些选项参数的校验
    check_options(options);

    // -v <loglevel>       set logging level
    int idx = locate_option(argc, argv, options, "loglevel");
    if(!idx)
        idx = locate_option(argc, argv, options, "v");
    if(idx && argv[idx + 1])
        // 设置loglevel
        opt_loglevel(NULL, "loglevel", argv[idx + 1]);

    // -report 存储完整的命令行和日志输出到当前目录的文件中
    // 将环境变量的FFREPORT设置为任何值都具有相同效果
    idx = locate_option(argc, argv, options, "report");
    char* env = getenv("FFREPORT");
    if(env || idx)
    {
        // 写入文件 等等...
        FILE* report_file = NULL;
        init_report(env, &report_file);
        if(report_file)
        {
            fprintf(report_file, "Command line:\n");
            for(int i = 0; i < argc; i++)
            {
                dump_argument(report_file, argv[i]);
                fputc(i < argc - 1 ? ' ' : '\n', report_file);
            }
            fflush(report_file);
        }
    }
    //freeenv_utf8(env);

    //-hide_banner <hide_banner>  do not show program banner
    idx = locate_option(argc, argv, options, "hide_banner");
    if(idx)
        hide_banner = 1;
}

static const OptionDef* find_option(const OptionDef* po, const char* name)
{
    if(*name == '/')
        name++;

    while(po->name) {
        const char* end;
        if(av_strstart(name, po->name, &end) && (!*end || *end == ':'))
            break;
        po++;
    }
    return po;
}

static int opt_has_arg(const OptionDef* o)
{
    if(o->type == OPT_TYPE_BOOL)
        return 0;
    if(o->type == OPT_TYPE_FUNC)
        return !!(o->flags & OPT_FUNC_ARG);
    return 1;
}

int locate_option(int argc, char** argv, const OptionDef* options,
    const char* optname)
{
    const OptionDef* po;
    int i;

    for(i = 1; i < argc; i++) {
        const char* cur_opt = argv[i];

        if(*cur_opt++ != '-')
            continue;

        po = find_option(options, cur_opt);
        if(!po->name && cur_opt[0] == 'n' && cur_opt[1] == 'o')
            po = find_option(options, cur_opt + 2);

        if((!po->name && !strcmp(cur_opt, optname)) ||
            (po->name && !strcmp(optname, po->name)))
            return i;

        if(!po->name || opt_has_arg(po))
            i++;
    }
    return 0;
}

static inline void prepare_app_arguments(int* argc_ptr, char*** argv_ptr)
{
    /* nothing to do */
}

static int init_parse_context(OptionParseContext* octx,
    const OptionGroupDef* groups, int nb_groups)
{
    memset(octx, 0, sizeof(*octx));

    // 输入输出参数链表
    octx->nb_groups = nb_groups;
    octx->groups = (OptionGroupList*)av_calloc(nb_groups, sizeof(*octx->groups));
    if(!octx->groups)
        return AVERROR(ENOMEM);

    for(int i = 0; i < octx->nb_groups; i++)
        octx->groups[i].group_def = &groups[i];

    // 全局参数对象
    static const OptionGroupDef global_group = { "global" };
    octx->global_opts.group_def = &global_group;
    octx->global_opts.arg = "";

    return 0;
}

static int finish_group(OptionParseContext* octx, int group_idx, const char* arg)
{
    OptionGroupList* l = &octx->groups[group_idx];
    OptionGroup* g;
    int ret;

    ret = GROW_ARRAY(l->groups, l->nb_groups);
    if(ret < 0)
        return ret;

    g = &l->groups[l->nb_groups - 1];

    *g = octx->cur_group;
    g->arg = arg;
    g->group_def = l->group_def;
    g->sws_dict = sws_dict;
    g->swr_opts = swr_opts;
    g->codec_opts = codec_opts;
    g->format_opts = format_opts;

    codec_opts = NULL;
    format_opts = NULL;
    sws_dict = NULL;
    swr_opts = NULL;

    memset(&octx->cur_group, 0, sizeof(octx->cur_group));

    return ret;
}

static int match_group_separator(const OptionGroupDef* groups, int nb_groups,
    const char* opt)
{
    int i;

    for(i = 0; i < nb_groups; i++) {
        const OptionGroupDef* p = &groups[i];
        if(p->sep && !strcmp(p->sep, opt))
            return i;
    }

    return -1;
}

static int add_opt(OptionParseContext* octx, const OptionDef* opt,
    const char* key, const char* val)
{
    int global = !(opt->flags & OPT_PERFILE);
    OptionGroup* g = global ? &octx->global_opts : &octx->cur_group;
    int ret;

    ret = GROW_ARRAY(g->opts, g->nb_opts);
    if(ret < 0)
        return ret;

    g->opts[g->nb_opts - 1].opt = opt;
    g->opts[g->nb_opts - 1].key = key;
    g->opts[g->nb_opts - 1].val = val;

    return 0;
}

static const AVOption* opt_find(void* obj, const char* name, const char* unit,
    int opt_flags, int search_flags)
{
    const AVOption* o = av_opt_find(obj, name, unit, opt_flags, search_flags);
    if(o && !o->flags)
        return NULL;
    return o;
}

#define FLAGS (o->type == AV_OPT_TYPE_FLAGS && (arg[0]=='-' || arg[0]=='+')) ? AV_DICT_APPEND : 0
int opt_default(void* optctx, const char* opt, const char* arg)
{
    const AVOption* o;
    int consumed = 0;
    char opt_stripped[128];
    const char* p;
    const AVClass* cc = avcodec_get_class(), * fc = avformat_get_class();
#if CONFIG_SWSCALE
    const AVClass* sc = sws_get_class();
#endif
#if CONFIG_SWRESAMPLE
    const AVClass* swr_class = swr_get_class();
#endif

    if(!strcmp(opt, "debug") || !strcmp(opt, "fdebug"))
        av_log_set_level(AV_LOG_DEBUG);

    if(!(p = strchr(opt, ':')))
        p = opt + strlen(opt);
    av_strlcpy(opt_stripped, opt, FFMIN(sizeof(opt_stripped), p - opt + 1));

    if((o = opt_find(&cc, opt_stripped, NULL, 0,
        AV_OPT_SEARCH_CHILDREN | AV_OPT_SEARCH_FAKE_OBJ)) ||
        ((opt[0] == 'v' || opt[0] == 'a' || opt[0] == 's') &&
            (o = opt_find(&cc, opt + 1, NULL, 0, AV_OPT_SEARCH_FAKE_OBJ)))) {
        av_dict_set(&codec_opts, opt, arg, FLAGS);
        consumed = 1;
    }
    if((o = opt_find(&fc, opt, NULL, 0,
        AV_OPT_SEARCH_CHILDREN | AV_OPT_SEARCH_FAKE_OBJ))) {
        av_dict_set(&format_opts, opt, arg, FLAGS);
        if(consumed)
            av_log(NULL, AV_LOG_VERBOSE, "Routing option %s to both codec and muxer layer\n", opt);
        consumed = 1;
    }
#if CONFIG_SWSCALE
    if(!consumed && (o = opt_find(&sc, opt, NULL, 0,
        AV_OPT_SEARCH_CHILDREN | AV_OPT_SEARCH_FAKE_OBJ))) {
        if(!strcmp(opt, "srcw") || !strcmp(opt, "srch") ||
            !strcmp(opt, "dstw") || !strcmp(opt, "dsth") ||
            !strcmp(opt, "src_format") || !strcmp(opt, "dst_format")) {
            av_log(NULL, AV_LOG_ERROR, "Directly using swscale dimensions/format options is not supported, please use the -s or -pix_fmt options\n");
            return AVERROR(EINVAL);
        }
        av_dict_set(&sws_dict, opt, arg, FLAGS);

        consumed = 1;
    }
#else
    if(!consumed && !strcmp(opt, "sws_flags")) {
        av_log(NULL, AV_LOG_WARNING, "Ignoring %s %s, due to disabled swscale\n", opt, arg);
        consumed = 1;
    }
#endif
#if CONFIG_SWRESAMPLE
    if(!consumed && (o = opt_find(&swr_class, opt, NULL, 0,
        AV_OPT_SEARCH_CHILDREN | AV_OPT_SEARCH_FAKE_OBJ))) {
        av_dict_set(&swr_opts, opt, arg, FLAGS);
        consumed = 1;
    }
#endif

    if(consumed)
        return 0;
    return AVERROR_OPTION_NOT_FOUND;
}

int split_commandline(OptionParseContext* octx, int argc, char* argv[],
    const OptionDef* options,
    const OptionGroupDef* groups, int nb_groups)
{
    /* perform system-dependent conversions for arguments list
    对参数列表执行依赖于系统的转换 如utf8 etc...,本实现为空 */
    prepare_app_arguments(&argc, &argv);

    // 初始化OptionParseContext 并设置初始值
    int ret = init_parse_context(octx, groups, nb_groups);
    if(ret < 0)
        return ret;

    av_log(NULL, AV_LOG_DEBUG, "Splitting the commandline.\n");

    int optindex = 1;
    int dashdash = -2;
    while(optindex < argc)
    {
        int ret = 0;

        const char* opt = argv[optindex++], * arg;
        av_log(NULL, AV_LOG_DEBUG, "Reading option '%s' ...", opt);
        if(opt[0] == '-' && opt[1] == '-' && !opt[2]) {
            dashdash = optindex;
            continue;
        }

        /* unnamed group separators, e.g. output filename */
        if(opt[0] != '-' || !opt[1] || dashdash + 1 == optindex)
        {
            ret = finish_group(octx, 0, opt);
            if(ret < 0)
                return ret;

            av_log(NULL, AV_LOG_DEBUG, " matched as %s.\n", groups[0].name);
            continue;
        }

        opt++;

        /* named group separators, e.g. -i */
        if((ret = match_group_separator(groups, nb_groups, opt)) >= 0)
        {
            GET_ARG(arg);
            ret = finish_group(octx, ret, arg);
            if(ret < 0)
                return ret;

            av_log(NULL, AV_LOG_DEBUG, " matched as %s with argument '%s'.\n", groups[ret].name, arg);
            continue;
        }

        /* normal options */
        const OptionDef* po = find_option(options, opt);
        if(po->name)
        {
            if(po->flags & OPT_EXIT)
            {
                /* optional argument, e.g. -h */
                arg = argv[optindex++];
            }
            else if(opt_has_arg(po))
            {
                GET_ARG(arg);
            }
            else 
            {
                arg = "1";
            }

            ret = add_opt(octx, po, opt, arg);
            if(ret < 0)
                return ret;

            av_log(NULL, AV_LOG_DEBUG, " matched as option '%s' (%s) with "
                "argument '%s'.\n", po->name, po->help, arg);
            continue;
        }

        /* AVOptions */
        if(argv[optindex])
        {
            ret = opt_default(NULL, opt, argv[optindex]);
            if(ret >= 0)
            {
                av_log(NULL, AV_LOG_DEBUG, " matched as AVOption '%s' with argument '%s'.\n", opt, argv[optindex]);
                optindex++;
                continue;
            }
            else if(ret != AVERROR_OPTION_NOT_FOUND)
            {
                av_log(NULL, AV_LOG_ERROR, "Error parsing option '%s' with argument '%s'.\n", opt, argv[optindex]);
                return ret;
            }
        }

        /* boolean -nofoo options */
        if(opt[0] == 'n' && opt[1] == 'o' && (po = find_option(options, opt + 2)) && po->name && po->type == OPT_TYPE_BOOL)
        {
            ret = add_opt(octx, po, opt, "0");
            if(ret < 0)
                return ret;

            av_log(NULL, AV_LOG_DEBUG, " matched as option '%s' (%s) with argument 0.\n", po->name, po->help);
            continue;
        }

        av_log(NULL, AV_LOG_ERROR, "Unrecognized option '%s'.\n", opt);
        return AVERROR_OPTION_NOT_FOUND;
    }

    if(octx->cur_group.nb_opts || codec_opts || format_opts)
        av_log(NULL, AV_LOG_WARNING, "Trailing option(s) found in the command: may be ignored.\n");

    av_log(NULL, AV_LOG_DEBUG, "Finished splitting the commandline.\n");
    return 0;
}

static int write_option(void* optctx, const OptionDef* po, const char* opt,
    const char* arg, const OptionDef* defs)
{
    /* new-style options contain an offset into optctx, old-style address of
     * a global var*/
    void* dst = po->flags & OPT_FLAG_OFFSET ?
        (uint8_t*)optctx + po->u.off : po->u.dst_ptr;
    char* arg_allocated = NULL;

    SpecifierOptList* sol = NULL;
    double num;
    int ret = 0;

    if(*opt == '/') {
        opt++;

        if(po->type == OPT_TYPE_BOOL) {
            av_log(NULL, AV_LOG_FATAL,
                "Requested to load an argument from file for a bool option '%s'\n",
                po->name);
            return AVERROR(EINVAL);
        }

        arg_allocated = file_read(arg);
        if(!arg_allocated) {
            av_log(NULL, AV_LOG_FATAL,
                "Error reading the value for option '%s' from file: %s\n",
                opt, arg);
            return AVERROR(EINVAL);
        }

        arg = arg_allocated;
    }

    if(po->flags & OPT_FLAG_SPEC) {
        char* p = strchr((char*)opt, ':');
        char* str;

        sol = (SpecifierOptList*)dst;
        ret = GROW_ARRAY(sol->opt, sol->nb_opt);
        if(ret < 0)
            goto finish;

        str = av_strdup(p ? p + 1 : "");
        if(!str) {
            ret = AVERROR(ENOMEM);
            goto finish;
        }
        sol->opt[sol->nb_opt - 1].specifier = str;
        dst = &sol->opt[sol->nb_opt - 1].u;
    }

    if(po->type == OPT_TYPE_STRING) {
        char* str;
        if(arg_allocated) {
            str = arg_allocated;
            arg_allocated = NULL;
        }
        else
            str = av_strdup(arg);
        av_freep(dst);

        if(!str) {
            ret = AVERROR(ENOMEM);
            goto finish;
        }

        *(char**)dst = str;
    }
    else if(po->type == OPT_TYPE_BOOL || po->type == OPT_TYPE_INT) {
        ret = parse_number(opt, arg, OPT_TYPE_INT64, INT_MIN, INT_MAX, &num);
        if(ret < 0)
            goto finish;

        *(int*)dst = num;
    }
    else if(po->type == OPT_TYPE_INT64) {
        ret = parse_number(opt, arg, OPT_TYPE_INT64, INT64_MIN, INT64_MAX, &num);
        if(ret < 0)
            goto finish;

        *(int64_t*)dst = num;
    }
    else if(po->type == OPT_TYPE_TIME) {
        ret = av_parse_time((int64_t*)dst, arg, 1);
        if(ret < 0) {
            av_log(NULL, AV_LOG_ERROR, "Invalid duration for option %s: %s\n",
                opt, arg);
            goto finish;
        }
    }
    else if(po->type == OPT_TYPE_FLOAT) {
        ret = parse_number(opt, arg, OPT_TYPE_FLOAT, -INFINITY, INFINITY, &num);
        if(ret < 0)
            goto finish;

        *(float*)dst = num;
    }
    else if(po->type == OPT_TYPE_DOUBLE) {
        ret = parse_number(opt, arg, OPT_TYPE_DOUBLE, -INFINITY, INFINITY, &num);
        if(ret < 0)
            goto finish;

        *(double*)dst = num;
    }
    else {
        int ret;

        av_assert0(po->type == OPT_TYPE_FUNC && po->u.func_arg);

        ret = po->u.func_arg(optctx, opt, arg);
        if(ret < 0) {
            av_log(NULL, AV_LOG_ERROR,
                "Failed to set value '%s' for option '%s': %s\n",
                arg, opt, av_err2str(ret));
            goto finish;
        }
    }
    if(po->flags & OPT_EXIT) {
        ret = AVERROR_EXIT;
        goto finish;
    }

    if(sol) {
        sol->type = po->type;
        sol->opt_canon = (po->flags & OPT_HAS_CANON) ?
            find_option(defs, po->u1.name_canon) : po;
    }

finish:
    av_freep(&arg_allocated);
    return ret;
}

int parse_optgroup(void* optctx, OptionGroup* g, const OptionDef* defs)
{
    int i, ret;

    av_log(NULL, AV_LOG_DEBUG, "Parsing a group of options: %s %s.\n",
        g->group_def->name, g->arg);

    for(i = 0; i < g->nb_opts; i++) {
        Option* o = &g->opts[i];

        if(g->group_def->flags &&
            !(g->group_def->flags & o->opt->flags)) {
            av_log(NULL, AV_LOG_ERROR, "Option %s (%s) cannot be applied to "
                "%s %s -- you are trying to apply an input option to an "
                "output file or vice versa. Move this option before the "
                "file it belongs to.\n", o->key, o->opt->help,
                g->group_def->name, g->arg);
            return AVERROR(EINVAL);
        }

        av_log(NULL, AV_LOG_DEBUG, "Applying option %s (%s) with argument %s.\n",
            o->key, o->opt->help, o->val);

        ret = write_option(optctx, o->opt, o->key, o->val, defs);
        if(ret < 0)
            return ret;
    }

    av_log(NULL, AV_LOG_DEBUG, "Successfully parsed a group of options.\n");

    return 0;
}

void uninit_opts(void)
{
    av_dict_free(&swr_opts);
    av_dict_free(&sws_dict);
    av_dict_free(&format_opts);
    av_dict_free(&codec_opts);
}


void uninit_parse_context(OptionParseContext* octx)
{
    int i, j;

    for(i = 0; i < octx->nb_groups; i++) {
        OptionGroupList* l = &octx->groups[i];

        for(j = 0; j < l->nb_groups; j++) {
            av_freep(&l->groups[j].opts);
            av_dict_free(&l->groups[j].codec_opts);
            av_dict_free(&l->groups[j].format_opts);

            av_dict_free(&l->groups[j].sws_dict);
            av_dict_free(&l->groups[j].swr_opts);
        }
        av_freep(&l->groups);
    }
    av_freep(&octx->groups);

    av_freep(&octx->cur_group.opts);
    av_freep(&octx->global_opts.opts);

    uninit_opts();
}

int grow_array(void** array, int elem_size, int* size, int new_size)
{
    if(new_size >= INT_MAX / elem_size) {
        av_log(NULL, AV_LOG_ERROR, "Array too big.\n");
        return AVERROR(ERANGE);
    }
    if(*size < new_size) {
        uint8_t* tmp = (uint8_t*)av_realloc_array(*array, new_size, elem_size);
        if(!tmp)
            return AVERROR(ENOMEM);
        memset(tmp + *size * elem_size, 0, (new_size - *size) * elem_size);
        *size = new_size;
        *array = tmp;
        return 0;
    }
    return 0;
}

int parse_number(const char* context, const char* numstr, enum OptionType type,
    double min, double max, double* dst)
{
    char* tail;
    const char* error;
    double d = av_strtod(numstr, &tail);
    if(*tail)
        error = "Expected number for %s but found: %s\n";
    else if(d < min || d > max)
        error = "The value for %s was %s which is not within %f - %f\n";
    else if(type == OPT_TYPE_INT64 && (int64_t)d != d)
        error = "Expected int64 for %s but found %s\n";
    else if(type == OPT_TYPE_INT && (int)d != d)
        error = "Expected int for %s but found %s\n";
    else {
        *dst = d;
        return 0;
    }

    av_log(NULL, AV_LOG_FATAL, error, context, numstr, min, max);
    return AVERROR(EINVAL);
}

/* read file contents into a string */
char* file_read(const char* filename)
{
    AVIOContext* pb = NULL;
    int ret = avio_open(&pb, filename, AVIO_FLAG_READ);
    AVBPrint bprint;
    char* str;

    if(ret < 0) {
        av_log(NULL, AV_LOG_ERROR, "Error opening file %s.\n", filename);
        return NULL;
    }

    av_bprint_init(&bprint, 0, AV_BPRINT_SIZE_UNLIMITED);
    ret = avio_read_to_bprint(pb, &bprint, SIZE_MAX);
    avio_closep(&pb);
    if(ret < 0) {
        av_bprint_finalize(&bprint, NULL);
        return NULL;
    }
    ret = av_bprint_finalize(&bprint, &str);
    if(ret < 0)
        return NULL;
    return str;
}

void* allocate_array_elem(void* ptr, size_t elem_size, int* nb_elems)
{
    void* new_elem;
    if(!(new_elem = av_mallocz(elem_size)) ||
        av_dynarray_add_nofree(ptr, nb_elems, new_elem) < 0)
        return NULL;
    return new_elem;
}

int filter_codec_opts(const AVDictionary* opts, enum AVCodecID codec_id,
    AVFormatContext* s, AVStream* st, const AVCodec* codec,
    AVDictionary** dst)
{
    AVDictionary* ret = NULL;
    const AVDictionaryEntry* t = NULL;
    int            flags = s->oformat ? AV_OPT_FLAG_ENCODING_PARAM
        : AV_OPT_FLAG_DECODING_PARAM;
    char          prefix = 0;
    const AVClass* cc = avcodec_get_class();

    if(!codec)
        codec = s->oformat ? avcodec_find_encoder(codec_id)
        : avcodec_find_decoder(codec_id);

    switch(st->codecpar->codec_type)
    {
    case AVMEDIA_TYPE_VIDEO:
        prefix = 'v';
        flags |= AV_OPT_FLAG_VIDEO_PARAM;
        break;
    case AVMEDIA_TYPE_AUDIO:
        prefix = 'a';
        flags |= AV_OPT_FLAG_AUDIO_PARAM;
        break;
    case AVMEDIA_TYPE_SUBTITLE:
        prefix = 's';
        flags |= AV_OPT_FLAG_SUBTITLE_PARAM;
        break;
    }

    while(t = av_dict_iterate(opts, t))
    {
        const AVClass* priv_class;
        char* p = strchr(t->key, ':');

        /* check stream specification in opt name */
        if(p) {
            int err = check_stream_specifier(s, st, p + 1);
            if(err < 0) {
                av_dict_free(&ret);
                return err;
            }
            else if(!err)
                continue;

            *p = 0;
        }

        if(av_opt_find(&cc, t->key, NULL, flags, AV_OPT_SEARCH_FAKE_OBJ) ||
            !codec ||
            ((priv_class = codec->priv_class) &&
                av_opt_find(&priv_class, t->key, NULL, flags,
                    AV_OPT_SEARCH_FAKE_OBJ)))
            av_dict_set(&ret, t->key, t->value, 0);
        else if(t->key[0] == prefix &&
            av_opt_find(&cc, t->key + 1, NULL, flags,
                AV_OPT_SEARCH_FAKE_OBJ))
            av_dict_set(&ret, t->key + 1, t->value, 0);

        if(p)
            *p = ':';
    }

    *dst = ret;
    return 0;
}

int setup_find_stream_info_opts(AVFormatContext* s,
    AVDictionary* codec_opts,
    AVDictionary*** dst)
{
    int ret;
    AVDictionary** opts;

    *dst = NULL;

    if(!s->nb_streams)
        return 0;

    opts = (AVDictionary**)av_calloc(s->nb_streams, sizeof(*opts));
    if(!opts)
        return AVERROR(ENOMEM);

    for(int i = 0; i < s->nb_streams; i++) {
        ret = filter_codec_opts(codec_opts, s->streams[i]->codecpar->codec_id,
            s, s->streams[i], NULL, &opts[i]);
        if(ret < 0)
            goto fail;
    }
    *dst = opts;
    return 0;
fail:
    for(int i = 0; i < s->nb_streams; i++)
        av_dict_free(&opts[i]);
    av_freep(&opts);
    return ret;
}

int check_stream_specifier(AVFormatContext* s, AVStream* st, const char* spec)
{
    int ret = avformat_match_stream_specifier(s, st, spec);
    if(ret < 0)
        av_log(s, AV_LOG_ERROR, "Invalid stream specifier: %s.\n", spec);
    return ret;
}

int read_yesno(void)
{
    int c = getchar();
    int yesno = (av_toupper(c) == 'Y');

    while(c != '\n' && c != EOF)
        c = getchar();

    return yesno;
}

double get_rotation(const int32_t* displaymatrix)
{
    double theta = 0;
    if(displaymatrix)
        theta = -round(av_display_rotation_get((int32_t*)displaymatrix));

    theta -= 360 * floor(theta / 360 + 0.9 / 360);

    if(fabs(theta - 90 * round(theta / 90)) > 2)
        av_log(NULL, AV_LOG_WARNING, "Odd rotation angle.\n"
            "If you want to help, upload a sample "
            "of this file to https://streams.videolan.org/upload/ "
            "and contact the ffmpeg-devel mailing list. (ffmpeg-devel@ffmpeg.org)");

    return theta;
}