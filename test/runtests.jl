using Convertsible
using Test

@testset "Convertsible.jl" begin

    @testset "Testing Type Conversions" begin
        @testset "Casting '$s' to Bool" for s in ["true", "True", "TRUE", "yeS", "YES", "yes"]
            @test convert(Bool, s) == true
        end
        @testset "Casting '$s' to Bool" for s in ["false", "False", "FALSE", "No", "NO", "zobi"]
            @test convert(Bool, s) == false
        end
    end    
    
end
