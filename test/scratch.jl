using ESGIWeatherLogistics2021: ESGIWeatherLogistics2021, Data
using Plots
using Dates
using DataFrames

const obs = Data.get_obs()
const obsdaily = ESGIWeatherLogistics2021.aggregate_daily_obs(obs)
const prices = Data.get_prices()

function plot_year(obsdaily, year; area, crop)
    obsidx = (obsdaily.area .== area) .& (Dates.year.(obsdaily.date) .== year)
    pricesidx = (prices.crop .== crop) .& (Dates.year.(prices.date) .== year)
    plot(obsdaily.date[obsidx], obsdaily.temperature_mean[obsidx])
    plot!(obsdaily.date[obsidx], obsdaily.temperature_minimum[obsidx])
    plot!(obsdaily.date[obsidx], obsdaily.temperature_maximum[obsidx])
    plot!(prices.date[pricesidx], 10 .* prices.price[pricesidx])
end
