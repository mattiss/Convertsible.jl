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
