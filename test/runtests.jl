using Convertsible
using Test
using DataFrames

@testset "Convertsible.jl" begin

    df_tests = DataFrame(Id = Int[], Name = String[], Age = Int[])
    push!(df_tests, [1 "John Doe" 35])

    @testset "Testing Playbook actions" begin
        @testset "Select columns" begin
            df = copy(df_tests)
            Convertsible.select_columns(df, columns = ["Name", "Id"])
            df_expected = DataFrame(Name = String[], Id = Int[])
            push!(df_expected, ["John Doe" 1])
            @test isequal(df, df_expected) 
        end

        @testset "Rename columns" begin
            df = copy(df_tests)
            Convertsible.rename_columns(df, rename = Dict("Name" => "NewName", "Age" => "NewAge"))
            df_expected = DataFrame(Id = Int[], NewName = String[], NewAge = Int[])
            push!(df_expected, [1 "John Doe" 35])
            @test isequal(df, df_expected) 
        end

        @testset "Normalize columns" begin
            df = copy(df_tests)
            df[!, :"bad looking column name"] = [0]
            Convertsible.normalize_columns(df, method = "pascal_case")
            df_expected = DataFrame(Id = Int[], Name = String[], Age = Int[], BadLookingColumnName = Int[])
            push!(df_expected, [1 "John Doe" 35 0])
            @test isequal(df, df_expected) 
        end
    end

    @testset "Testing Type Conversions" begin
        @testset "Casting '$s' to Bool" for s in ["true", "True", "TRUE", "yeS", "YES", "yes"]
            @test convert(Bool, s) == true
        end
        @testset "Casting '$s' to Bool" for s in ["false", "False", "FALSE", "No", "NO", "zobi"]
            @test convert(Bool, s) == false
        end
    end    
    
end
