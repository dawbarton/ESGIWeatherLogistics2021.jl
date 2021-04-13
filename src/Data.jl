module Data

using PyCall
using Dates
using DataFrames
using Serialization
using Scratch

# Raw data is expected to be in the rawdata folder with the structure

# rawdata
# └───ESGI_Observations
#     ├───Precipitation_Observations
#     │   ├───Spain
#     │   └───United Kingdom
#     └───Temperature_Observations
#         ├───Spain
#         └───United Kingdom

const OBS_RAWDATA = joinpath(@__DIR__, "..", "rawdata", "ESGI_Observations")
const OBS_PRECIP = joinpath(OBS_RAWDATA, "Precipitation_Observations")
const OBS_TEMP = joinpath(OBS_RAWDATA, "Temperature_Observations")

"""
    convert_obs_dir(dir)

Load observation data in NumPy format and return as a named tuple with fields `(country,
area, datetime, value)`.
"""
function convert_obs_dir(dir)
    NP = pyimport("numpy")
    pattern = r"\\(\w*).hourly_(.......)\.npy$"
    df = DateFormat("uuuyyyy")
    countries = String[]
    areas = String[]
    dates = DateTime[]
    values = Union{Missing, Float64}[]
    for country in readdir(dir)
        countrypath = joinpath(dir, country)
        if !isdir(countrypath)
            continue
        end
        for filename in readdir(countrypath; join=true)
            res = match(pattern, filename)
            if res === nothing
                @warn "File name did not match expected pattern" filename
                continue
            else
                (area, month) = res.captures
            end
            data = replace(NP.load(filename), NaN=>missing)'  # NaNs represent missing values
            basedate = DateTime(month, df)
            @assert size(data, 1) == 24  # 24 hours in a day
            interval = range(basedate; step=Hour(1), stop=basedate+Month(1)-Hour(1))
            n = length(interval)
            # Append
            append!(countries, fill(country, n))
            append!(areas, fill(area, n))
            append!(dates, interval)
            append!(values, vec(data)[1:n])  # the data is always stored as 31 days but in months with 30 days the last day is a duplicate of the 31st day of the previous month
        end
    end
    return (country=countries, area=areas, date=dates, value=values)
end

"""
    convert_obs()

Convert temperature and precipitation data from NumPy format into a `DataFrame`.
"""
function convert_obs()
    p = convert_obs_dir(OBS_PRECIP)
    precip = DataFrame((country=p.country, area=p.area, date=p.date, precipitation=p.value))
    t = convert_obs_dir(OBS_TEMP)
    temp = DataFrame((country=t.country, area=t.area, date=t.date, temperature=t.value))
    return outerjoin(precip, temp; on=[:country, :area, :date])
end

"""
    get_obs()

Return temperature and precipitation data as a `DataFrame`. Data is cached in a scratch
space for speed.
"""
function get_obs()
    path = @get_scratch!("observations")
    obsfile = joinpath(path, "observations.jls")
    if isfile(obsfile)
        return Serialization.deserialize(obsfile)
    else
        obs = convert_obs()
        Serialization.serialize(obsfile, obs)
        return obs
    end
end

end
