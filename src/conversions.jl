
function Base.convert(Bool, s::String)
    res = false
    if lowercase(s) in ["true","yes"]
        res = true
    end
    return res
end

