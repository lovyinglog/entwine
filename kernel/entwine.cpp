/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "entwine.hpp"

#include <csignal>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#ifndef _WIN32
#include <pdal/util/Utils.hpp>

#include <execinfo.h>
#include <unistd.h>
#include <dlfcn.h>
#endif

namespace
{
    std::string getUsageString()
    {
        return
            "\tUsage: entwine <kernel> <options>\n"
            "\tKernels:\n"
            "\t\tbuild\n"
            "\t\t\tBuild (or continue to build) an index\n"
            "\t\tinfer\n"
            "\t\t\tAggregate information for an unindexed dataset\n"
            "\t\tmerge\n"
            "\t\t\tMerge colocated previously built subsets\n";
    }

    std::mutex mutex;
}

int main(int argc, char** argv)
{
#ifndef _WIN32
    signal(SIGSEGV, [](int sig)
    {
        std::lock_guard<std::mutex> lock(mutex);
        std::cout << "Got error " << sig << std::endl;

        void* buffer[32];
        const std::size_t size(backtrace(buffer, 32));
        char** symbols(backtrace_symbols(buffer, size));

        std::vector<std::string> lines;

        for (std::size_t i(0); i < size; ++i)
        {
            std::string symbol(symbols[i]);
            Dl_info info;

            if (dladdr(buffer[i], &info))
            {
                const auto demangled(pdal::Utils::demangle(info.dli_sname));

                const std::size_t offset(
                        static_cast<char*>(buffer[i]) -
                        static_cast<char*>(info.dli_saddr));

                // Replace the address and mangled name with a human-readable
                // name.
                std::string prefix(std::to_string(i) + "  ");
                const std::size_t pos(symbol.find("0x"));
                if (pos != std::string::npos)
                {
                    prefix = symbol.substr(0, pos);
                }

                lines.push_back(prefix + demangled + " + " +
                        std::to_string(offset));
            }
            else
            {
                lines.push_back(symbol);
            }
        }

        for (const auto& l : lines) std::cout << l << std::endl;

        free(symbols);
        exit(1);
    });
#endif

    if (argc < 2)
    {
        std::cout << "Kernel type required\n" << getUsageString() << std::endl;
        exit(1);
    }

    const std::string kernel(argv[1]);

    std::vector<std::string> args;

    for (int i(2); i < argc; ++i)
    {
        std::string arg(argv[i]);

        if (arg.size() > 2 && arg.front() == '-' && std::isalpha(arg[1]))
        {
            // Expand args of the format "-xvalue" to "-x value".
            args.push_back(arg.substr(0, 2));
            args.push_back(arg.substr(2));
        }
        else
        {
            args.push_back(argv[i]);
        }
    }

    try
    {
        if (kernel == "build")
        {
            Kernel::build(args);
        }
        else if (kernel == "merge")
        {
            Kernel::merge(args);
        }
        else if (kernel == "infer")
        {
            Kernel::infer(args);
        }
        else if (kernel == "convert")
        {
            Kernel::convert(args);
        }
        else
        {
            if (kernel != "help" && kernel != "-h" && kernel != "--help")
            {
                std::cout << "Invalid kernel type\n";
            }

            std::cout << getUsageString() << std::endl;
        }
    }
    catch (std::runtime_error& e)
    {
        std::cout << "Encountered an error: " << e.what() << std::endl;
        std::cout << "Exiting." << std::endl;
        return 1;
    }

    return 0;
}

