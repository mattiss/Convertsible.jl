using Convertsible
using Test
using DataFrames

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
        @testset "Testing '$s'" for s in ["playbooks\\csv2parquet_playbook.yml", "playbooks\\data_transforms.yml"]
            Convertsible.run_playbook(s)
        end
    end
    
end
