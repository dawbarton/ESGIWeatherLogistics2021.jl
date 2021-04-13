module ESGIWeatherLogistics2021

include("Data.jl")

using DataFrames: sort!, combine, groupby, transform
using Dates: Date
using Statistics: mean

function aggregate_daily_obs(obs)
    return sort!(
        combine(
            groupby(
                transform(obs, :date => x -> Date.(x); renamecols=false),
                [:country, :area, :date],
            ),
            :precipitation => sum,
            :temperature => mean,
            :temperature => minimum,
            :temperature => maximum,
        ),
        [:country, :area, :date],
    )
end

end
