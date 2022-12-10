ALLOWED_ACTIONS = ["load_csv","select_columns","normalize_columns", "rename_columns", "convert_to_float", "write_csv", "write_parquet"]

using DataFrames
using FilePaths, CSV
function load_csv(df::AbstractDataFrame; filepath, kwargs...)
    printstyled("  Loading $filepath...\n";color = :yellow)
    empty!(df)
    append!(df, CSV.read(filepath, DataFrame; stringtype=String, kwargs...))
    printstyled("  Done.\n";color = :yellow)
end

function write_csv(df::AbstractDataFrame; filepath, kwargs...)
    printstyled("  Writing $filepath...\n";color = :yellow)
    CSV.write(filepath, df)
    printstyled("  Done.\n";color = :yellow)
end

using Parquet
function write_parquet(df::AbstractDataFrame; filepath, kwargs...)
    printstyled("  Test Writing $filepath...\n";color = :yellow)
    display(df)
    Parquet.write_parquet(filepath, df)
    printstyled("  Done.\n";color = :yellow)
end

function select_columns(df::AbstractDataFrame; columns, kwargs...)
    printstyled("  Selecting columns $columns...\n";color = :yellow)
    select!(df, columns)
    printstyled("  Done.\n";color = :yellow)
end

function pascal_case(cur_string::AbstractString)
    replace(
        titlecase(cur_string), " " => ""
    )
end
function normalize_columns(df::AbstractDataFrame; method, kwargs...)
    printstyled("  Normalizing column names [$(method)]...\n";color = :yellow)
    normalize_function = getfield(Convertsible, Symbol(method))
    rename!(normalize_function, df)
    printstyled("  Done.\n";color = :yellow)
end

function rename_columns(df::AbstractDataFrame; rename, kwargs...)
    printstyled("  Renaming columns $(keys(rename))...\n";color = :yellow)
    for (old, new) in pairs(rename)
        rename_dict = [(x => replace.(x, old .=> new)) for x in names(df, old)]
        rename!(df, rename_dict)
    end   
    printstyled("  Done.\n";color = :yellow)
end

function convert_to_float(df::AbstractDataFrame; columns, kwargs...)
    printstyled("  Converting columns $columns to float...\n";color = :yellow)
    df[!, columns] = convert.(Float64, df[!, columns])
    printstyled("  Done.\n";color = :yellow)
end
