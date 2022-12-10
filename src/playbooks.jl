# maintaining the order from the YAML file
using YAML, OrderedCollections, Mustache, DataFrames
include("playbook_actions.jl")

function render_playbook(playbook_filepath::String; vars::AbstractDict = Dict())
    playbook = YAML.load_file(playbook_filepath)
    playbook = playbook[1]
    # Override variables defined in the playbook file by the ones passed as argument
    playbook_variables = merge(isnothing(playbook["vars"]) ? Dict() : playbook["vars"], vars)
    playbook = YAML.load(Mustache.render_from_file(playbook_filepath, playbook_variables); dicttype=OrderedDict{String,Any})
    playbook = playbook[1]
    return playbook
end

function run_playbook(playbook_filepath::String; kwargs...)
    playbook = render_playbook(playbook_filepath; kwargs...)
    print("Running playbook : ")
    printstyled("$(playbook["name"])\n";color = :cyan)
    if @isdefined(vars)
        println("with arguments: ")
        display(vars)
    end

    df = DataFrame()
    for task in playbook["tasks"]
        name = pop!(task, "name")
        println(name)
        for (fn_name, fn_args) in task
            if fn_name in ALLOWED_ACTIONS
                run_task(df, fn_name, fn_args)
            else
                println(fn_name * " is not a recognized task name. skipping.")
                println("Allowed actions are :")
                display(ALLOWED_ACTIONS)
            end
        end
    end
    println()
end

function run_task(df::DataFrame, fn_name, fn_args)
    fn = getfield(Convertsible, Symbol(fn_name))
    # cast Dictionary keys as Symbol 
    fn_args = Dict(Symbol(key)=>val for (key,val) in fn_args) 
    fn(df; fn_args...)
end

# run_playbook("playbook_example.yml")


