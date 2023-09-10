
using Test

@testset "Convertsible.jl" begin

    @testset "Testing Playbook rendering" begin

        @testset "csv2parquet" begin
            playbook_filepath = "playbooks/csv2parquet_playbook.yml"
            input_filepath = "data/csv/sample_data_test.csv"
            actual_filepath = "output/actual/01_csv2parquet_sample_data_test.parquet"
    
            vars = Dict()
            push!(vars, "csv_filepath"=>input_filepath)
            push!(vars, "parquet_filepath"=>actual_filepath)
            display(vars)
                
            playbook = Convertsible.render_playbook(playbook_filepath;vars)
            display(playbook["tasks"])
        end
    end
end