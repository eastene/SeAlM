#include <iostream>
#include <getopt.h>
#include "wrapped_mapper.hpp"

int main(int argc, char** argv) {
    CLIOptions opts;
    int c;

    while (true)
    {
        static struct option long_options[] =
                {
                        /* These options set a flag. */
                        {"verbose", no_argument,       &opts.verbose_flag, 1},
                        {"quiet",   no_argument,       &opts.verbose_flag, 0},
                        /* These options don’t set a flag.
                           We distinguish them by their indices. */
                        {"input_file",     required_argument,       0, 'i'},
                        {"reference",  required_argument,       0, 'x'},
                        {"output_file",  required_argument, 0, 's'},
                        {"metrics_file",  required_argument, 0, 'e'},
                        {"batch_size",  optional_argument, 0, 'b'},
                        {"manager_type",    required_argument, 0, 'm'},
                        {"cache_type",    required_argument, 0, 'c'},
                        {0, 0, 0, 0}
                };

        /* getopt_long stores the option index here. */
        int option_index = 0;

        c = getopt_long (argc, argv, "i:x:s:b:m:c",
                         long_options, &option_index);

        /* Detect the end of the options. */
        if (c == -1)
            break;

        switch (c)
        {
            case 0:
                /* If this option set a flag, do nothing else now. */
                if (long_options[option_index].flag != 0)
                    break;
                printf ("option %s", long_options[option_index].name);
                if (optarg)
                    printf (" with arg %s", optarg);
                printf ("\n");
                break;

            case 'i':
                if (optarg)
                    opts.input_file = optarg;
                break;

            case 'x':
                if (optarg)
                    opts.reference = optarg;
                break;

            case 's':
                if (optarg)
                    opts.output_file = optarg;
                break;

            case 'e':
                if (optarg)
                    opts.metrics_file = optarg;
                break;

            case 'b':
                if (optarg)
                    opts.batch_size = std::stoi(optarg);
                break;

            case 'm':
                if (optarg)
                    opts.manager_type = std::stoi(optarg);
                break;

            case 'c':
                if (optarg)
                    opts.cache_type = std::stoi(optarg);
                break;

            case '?':
                /* getopt_long already printed an error message. */
                break;

            default:
                std::cout << "Error in: --" << long_options[option_index].name << std::endl;
                abort ();
        }
    }

    assert(!opts.input_file.empty());
    assert(!opts.reference.empty());
    assert(!opts.output_file.empty());

    /* Instead of reporting ‘--verbose’
       and ‘--brief’ as they are encountered,
       we report the final status resulting from them. */
    if (opts.verbose_flag)
        puts ("verbose flag is set");

    /* Print any remaining command line arguments (not options). */
    if (optind < argc)
    {
        printf ("non-option ARGV-elements: ");
        while (optind < argc)
            printf ("%s ", argv[optind++]);
        putchar ('\n');
    }

    // perform alignment with args
    WrappedMapper wm(opts);
    wm.run_alignment();
    // print final metrics
    std::cout << wm;
    // log per-batch metrics
    std::ofstream log(opts.metrics_file);
    log << wm.prepare_log();
    log.close();
}