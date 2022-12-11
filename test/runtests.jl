using Convertsible
using Test
using DataFrames
using Mmap

@testset "Convertsible.jl" begin

    @testset "Testing Playbook actions" begin

        @testset "Load csv" begin
            df = DataFrame()
            Convertsible.load_csv(df, filepath="data/csv/sample_data.csv")
            df_expected = DataFrame(("Id"=>1),("Name"=>"John Doe"),("Age"=>35))
            @test isequal(df_expected, df) 
        end

        @testset "Select columns" begin
            df = DataFrame(("Id"=>1),("Name"=>"John Doe"),("Age"=>35))
            Convertsible.select_columns(df, columns=["Name", "Id"])
            df_expected = DataFrame(("Name"=>"John Doe"),("Id"=>1))
            @test isequal(df_expected, df) 
        end

        @testset "Rename columns" begin
            df = DataFrame(("Id"=>1),("Name"=>"John Doe"),("Age"=>35))
            Convertsible.rename_columns(df, rename=Dict("Name" => "NewName", "Age" => "NewAge"))
            df_expected = DataFrame(("Id"=>1),("NewName"=>"John Doe"),("NewAge"=>35))
            @test isequal(df_expected, df) 
        end

        @testset "Normalize columns" begin
            df = DataFrame(("Id"=>1),("Name"=>"John Doe"),("Age"=>35),("bad looking column name"=>missing))
            Convertsible.normalize_columns(df, method="pascal_case")
            df_expected = DataFrame(("Id"=>1),("Name"=>"John Doe"),("Age"=>35),("BadLookingColumnName"=>missing))
            @test isequal(df_expected, df) 
        end

        @testset "Convert columns types" begin 
            df = DataFrame(("Id"=>1),("Name"=>"John Doe"),("Age"=>35))
            Convertsible.convert_columns(df, types = Dict("Id" => "String", "Name" => "Bool", "Age" => "Double"))
            df_expected = DataFrame(("Id"=>"1"),("Name"=>false),("Age"=>35.0))
            @test isequal(df_expected, df) 
        end

        @testset "Load csv with partially missing data" begin
            df = DataFrame()
            Convertsible.load_csv(df, filepath="data/csv/partially_missing_data.csv")
            df_expected = DataFrame(Id = Int[], Name = String[], Age = Union{Missing, Int}[], Income = Int[])
            push!(df_expected, [1 "John Doe" missing 100000])
            push!(df_expected, [2 "Harry Cover" 24 230000])
            @test isequal(df_expected, df) 

            # Convertsible.convert_columns(df, types = Dict("Age" => "Double"))
            # df_expected = DataFrame(Id = Int[], Name = String[], Age = Float64[], Income = Int[])
            # push!(df_expected, [1 "John Doe" missing 100000])
            # @test isequal(df_expected, df) 
        end       

        @testset "Load csv with totally missing data" begin
            df = DataFrame()
            Convertsible.load_csv(df, filepath="data/csv/totally_missing_data.csv")
            df_expected = DataFrame(Id = Int[], Name = String[], Age = Union{Missing, Int}[], Income = Int[])
            push!(df_expected, [1 "John Doe" missing 100000])
            @test isequal(df_expected, df) 

            # Convertsible.convert_columns(df, types = Dict("Age" => "Double"))
            # df_expected = DataFrame(Id = Int[], Name = String[], Age = Float64[], Income = Int[])
            # push!(df_expected, [1 "John Doe" missing 100000])
            # @test isequal(df_expected, df) 
        end       
    end

    @testset "Testing Type Conversions" begin
        @testset "Casting '$s' to Bool" for s in ["true", "True", "TRUE", "yeS", "YES", "yes"]
            @test convert(Bool, s) == true
        end
        @testset "Casting '$s' to Bool" for s in ["false", "False", "FALSE", "No", "NO", "zobi"]
            @test convert(Bool, s) == false
        end
        
        @testset "Casting Number to String" begin
            @test convert(String, 12345) == "12345"
            @test convert(String, 35.0) == "35.0"
        end
    end    

    @testset "Testing Playbooks" begin

        playbook = "playbooks/csv2parquet_playbook.yml"
        input_filepath = "data/csv/sample_data.csv"
        actual_filepath = "output/actual/01_csv2parquet_sample_data.parquet"
        expected_filepath = "output/expected/01_csv2parquet_sample_data.parquet"
        @testset "Testing '$playbook'" begin
            vars = Dict()
            push!(vars, "csv_filepath"=>input_filepath)
            push!(vars, "parquet_filepath"=>actual_filepath)
            Convertsible.run_playbook(playbook; vars=vars)
            f_actual = open(actual_filepath)
            f_expected = open(expected_filepath);
            @test Mmap.mmap(f_actual) == Mmap.mmap(f_expected)
        end

        playbook = "playbooks/data_transforms.yml"
        input_filepath = "data/csv/sample_data.csv"
        actual_filepath_csv = "output/actual/02_data_transforms_sample_data.csv"
        actual_filepath_parquet = "output/actual/02_data_transforms_sample_data.parquet"
        expected_filepath = "output/expected/02_data_transforms_sample_data.parquet"
        @testset "Testing '$playbook'" begin
            vars = Dict()
            push!(vars, "csv_filepath"=>input_filepath)
            push!(vars, "output_filepath"=>actual_filepath_csv)
            push!(vars, "parquet_filepath"=>actual_filepath_parquet)
            Convertsible.run_playbook(playbook; vars=vars)
            f_actual = open(actual_filepath_parquet)
            f_expected = open(expected_filepath);
            @test Mmap.mmap(f_actual) == Mmap.mmap(f_expected)
        end

    end
    
end
