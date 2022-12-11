
function Base.convert(::Type{Bool}, s::String)
    res = false
    if lowercase(s) in ["true","yes"]
        res = true
    end
    return res
end


function Base.convert(::Type{String}, n::Number)
    return string(n)
end
