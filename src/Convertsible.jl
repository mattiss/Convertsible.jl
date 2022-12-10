module Convertsible

export ALLOWED_ACTIONS, main, convert
include("playbooks.jl")

using ArgParse
function parse_commandline()
    s = ArgParseSettings()

    @add_arg_table s begin
        "sourceFolder"
            help = "the folder where the csv files are located"
            required = true
        "destinationFolder"
            help = "the folder where to transfer the parquet file(s). can be a S3 location"
            required = true
        "--filterpattern", "-f"
            help = "the pattern regex to use when filtering results"
            default = raw"(.)*\.csv"
        "--playbook", "-p"
            help = "the path to the yaml file describing the various transforms to apply"
            required = true
    end

    return parse_args(s)
end

function main()
    parsed_args = parse_commandline()
    println("Parsed args:")
    display(parsed_args)
    println()

    csv_rootpath = parsed_args["sourceFolder"]
    parquet_rootpath = parsed_args["destinationFolder"]
    csv_pattern = Regex(parsed_args["filterpattern"])
    playbook_filepath = parsed_args["playbook"]

    mkpath(parquet_rootpath)
    csv_extension  = raw".csv"
    parquet_extension  = raw".parquet"
    for (root, dirs, files) in walkdir(csv_rootpath)
        for file in files
            if occursin(csv_pattern, file) 
                csv_filepath = joinpath(root, file)
                parquet_filepath = joinpath(parquet_rootpath,replace(file, csv_extension => parquet_extension))
                # println(csv_filepath * "=>" * string(parquet_filepath)) 
                vars = Dict("csv_filepath" => csv_filepath, "parquet_filepath" => parquet_filepath)
                # display(vars)
                run_playbook(playbook_filepath, vars=vars)
                # convertsible.transform(csv_filepath, parquet_filepath)
            end
        end
    end    
end

function julia_main()::Cint
    try
        main()
    catch
        Base.invokelatest(Base.display_error, Base.catch_stack())
        return 1
    end
    return 0
end


end
