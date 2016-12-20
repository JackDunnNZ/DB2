module DB2

using DataFrames, StatsBase

export personGenerator, calculatePremiums

premiums_df = DataFrame(State = String[],
                          Catastrophic = Int64[],
                          Bronze = Int64[],
                          Silver = Int64[],
                          Gold = Int64[],
                          Platinum = Int64[],
                          Suburban_penalty = Float32[],
                          Rural_Penalty = Float32[],
                          Tobacco_penalty = Float32[],
                          Population = Int64[])

push!(premiums_df, ["Alabama",193,217,269,314,371,1,1,1.5,4871547])
push!(premiums_df, ["Alaska",364,475,583,689,689,1,1,1.5,739818])
push!(premiums_df, ["Arizona",169,229,279,319,334,1,1.7,1.5,6927347])
push!(premiums_df, ["Arkansas",191,255,326,375,375,1.1,1,1.5,2989573])
push!(premiums_df, ["California",223,249,300,358,407,1.4,1.3,1,39497345])
push!(premiums_df, ["Colorado",222,262,323,366,405,0.9,1.3,1.5,5557560])
push!(premiums_df, ["Connecticut",217,290,372,411,490,1.2,1,1.5,3587010])
push!(premiums_df, ["Delaware",219,252,324,370,439,1,1,1.5,955900])
push!(premiums_df, ["District of Columbia",175,228,294,361,440,1,1,1,684620])
push!(premiums_df, ["Florida",249,303,369,419,487,1.2,1.1,1.5,20636975])
push!(premiums_df, ["Georgia",208,247,307,365,443,1,1.1,1.5,10332588])
push!(premiums_df, ["Hawaii",155,171,206,252,318,1,1,1.5,1442949])
push!(premiums_df, ["Idaho",210,238,289,342,406,1.1,1.2,1.5,1675054])
push!(premiums_df, ["Illinois" ,210,238,289,342,406,1.1,1.2,1.5,12837801])
push!(premiums_df, ["Indiana",232,286,352,425,647,0.9,0.8,1.5,6641480])
push!(premiums_df, ["Iowa",240,246,310,364,524,0.8,1.2,1.5,3138317])
push!(premiums_df, ["Kansas",158,205,245,286,341,1,1,1.5,2920775])
push!(premiums_df, ["Kentucky",181,220,277,326,345,1.2,1.2,1.5,4437567])
push!(premiums_df, ["Louisiana",209,274,359,411,454,1,1.1,1.5,4692458])
push!(premiums_df, ["Maine",205,272,342,425,425,1.2,1.1,1.5,1328400])
push!(premiums_df, ["Maryland",174,207,269,317,385,1,1,1.5,6037456])
push!(premiums_df, ["Massachusetts",254,288,345,440,526,1,1,1,6833720])
push!(premiums_df, ["Michigan",197,252,305,371,412,1,1.1,1.5,9928846])
push!(premiums_df, ["Minnesota",142,195,232,284,312,1.1,1.5,1.5,5522063])
push!(premiums_df, ["Mississippi",238,243,315,353,384,1.1,1,1.5,2991223])
push!(premiums_df, ["Missouri",216,245,306,358,330,0.9,1.1,1.5,6103517])
push!(premiums_df, ["Montana",185,226,273,331,384,1,1,1.5,1042646])
push!(premiums_df, ["Nebraska",206,261,319,375,514,1,1,1.5,1909400])
push!(premiums_df, ["Nevada",220,247,284,321,364,1.3,1.8,1.5,2943409])
push!(premiums_df, ["New Hampshire",194,254,329,400,665,1,1,1.5,1333220])
push!(premiums_df, ["New Jersey",272,327,360,454,554,1,1,1,8977182])
push!(premiums_df, ["New Mexico",185,205,253,302,384,1.2,1.4,1.5,2084651])
push!(premiums_df, ["New York",267,333,368,471,540,1,1,1,19842724])
push!(premiums_df, ["North Carolina" ,206,270,343,396,447,1,1,1.5,10145217])
push!(premiums_df, ["North Dakota",189,263,318,365,365,1.1,1.1,1.5,773814])
push!(premiums_df, ["Ohio",196,258,321,373,540,1.1,1.1,1.5,11629848])
push!(premiums_df, ["Oklahoma",173,220,287,359,504,1,1,1.5,3943066])
push!(premiums_df, ["Oregon",201,212,262,310,366,1.1,1.1,1.5,4086752])
push!(premiums_df, ["Pennsylvania",194,261,313,354,454,0.9,0.8,1.5,12811239])
push!(premiums_df, ["Rhode Island",199,228,284,340,340,1,1,1,1057689])
push!(premiums_df, ["South Carolina",207,268,308,365,684,1,1,1.5,4963132])
push!(premiums_df, ["South Dakota",203,243,289,368,421,1.1,1,1.5,863634])
push!(premiums_df, ["Tennessee",171,200,258,344,438,0.9,1,1.5,6652819])
push!(premiums_df, ["Texas",248,269,328,388,493,0.8,0.9,1.5,27959150])
push!(premiums_df, ["Utah",197,207,256,299,272,1.1,1.1,1.5,3047340])
push!(premiums_df, ["Vermont",223,383,455,546,635,1,1,1,625317])
push!(premiums_df, ["Virginia",193,246,307,375,517,1,1,1.5,8437888])
push!(premiums_df, ["Washington",229,242,305,364,420,1.1,1,1.5,7277536])
push!(premiums_df, ["West Virginia",217,241,289,350,350,1.1,1.1,1.5,1839505])
push!(premiums_df, ["Wisconsin",228,308,373,447,564,0.9,0.8,1.5,5783242])
push!(premiums_df, ["Wyoming" ,336,394,456,537,580,1,1.1,1.5,587910])

function personGenerator(n::Int64)
    output_df = DataFrame(First_name = String[],
                          Last_name = String[],
                          Age = Float32[],
                          Gender = String[],
                          Income = Float32[],
                          State = String[],
                          Urban_rural = String[],
                          Tobacco_use = String[],
                          Family_individual = String[],
                          Plan_level = String[],
                          Existing_plan = String[],
                          Last_paid = Date[])
    for i = 1:n
        first = randstring()
        last = randstring()
        age = 50+randn()*15
        while (age < 20) | (age > 85)
            age = 50+randn()*15
        end
        gender = sample(["M", "F"], WeightVec([49.2, 50.8]))
        income = max(age*1000+randn()*10000,0)
        state = sample(premiums_df[:State], WeightVec(premiums_df[:Population]))
        urbanRural = sample(["Urban", "Suburban", "Rural"], WeightVec([26, 53, 21]))
        tobaccoChance = 19
        if (age < 25) | (age > 65)
            if gender == "M"
                tobaccoChance = 14
            else
                tobaccoChance = 11
            end
        else
            if gender == "F"
                tobaccoChance = 16
            end
        end
        tobacco = sample(["Yes", "No"], WeightVec([tobaccoChance, 100-tobaccoChance]))
        familyChance = max(0, min(100, income/1000))
        family = sample(["Family", "Individual"], WeightVec([familyChance, 100-familyChance]))
        plan = sample(["Bronze", "Silver", "Gold", "Platinum", "Catastrophic"])
        existingPlan = sample(["Yes", "No"])
        lastPaid = NA
        if existingPlan == "Yes"
            lastPaid = Date(rand(2014:2016),rand(1:12),rand(1:28))
        end

        push!(output_df,
              [first, last, age, gender, income, state,
              urbanRural, tobacco, family, plan, existingPlan, lastPaid])
    end

    return output_df
end



function calculatePremiums(input_df)


    function premiumCalculation(age, state, urbanRural, tobacco, family, plan)
        #Get my state info
        stateID = find(x->x==state,premiums_df[:State])
        if length(stateID)==1
            stateID = stateID[1]
        else
            stateID = 0
        end

        # Pulls the premium from your state and plan type
        basePremium = 0
        if stateID > 0
            if plan == "Bronze"
                basePremium = premiums_df[stateID, 3][1]
            elseif plan == "Silver"
                basePremium = premiums_df[stateID, 4][1]
            elseif plan == "Gold"
                basePremium = premiums_df[stateID, 5][1]
            elseif plan == "Platinum"
                basePremium = premiums_df[stateID, 6][1]
            elseif plan =="Catastrophic"
                basePremium = premiums_df[stateID, 2][1]
            end
        end

        # Adjusts premium for rural/suburban location
        premium = 0
        if urbanRural == "Suburban"
            premium = basePremium*premiums_df[stateID, 7][1]
        elseif urbanRural == "Rural"
            premium = basePremium*premiums_df[stateID, 8][1]
        elseif urbanRural == "Urban"
            premium = basePremium
        end

        # Adjusts premium for smokers
        if tobacco
             premium = premium*premiums_df[stateID, 9][1]
        end

        # Doubles premium if it's for a family
        if family
             premium = premium*2
        end

        # Adjusts premium for age
        ageMultiplier = 1
        newage = min(64, max(19, age))
        if age > 40
            ageMultiplier = 1 + 0.00285*(age-40)^2
        else
            ageMultiplier = 1 - .025*(40-age)
        end
        premium = premium*ageMultiplier

        #Finally, round
        premium = round(premium,2)
        return premium
    end

    numRecords = size(input_df,1)
    premiumOutput = zeros(numRecords)
    for i = 1:numRecords
        age = input_df[i,:Age]
        state = input_df[i,:State]
        urbanRural = input_df[i,:Urban_rural]
        tobacco = false
        if input_df[i,:Tobacco_use]=="Yes" tobacco = true end
        family = false
        if input_df[i,:Family_individual]=="Family" family = true end
        plan = input_df[i,:Plan_level]
        premiumOutput[i] = premiumCalculation(age, state, urbanRural, tobacco, family, plan)
    end

    return premiumOutput
end

#Importing claim history
# claimHeader = ["Date"  "Proivder" "Claim type" "Pre-authorization" "Dollar amount"]
# claim1 = [Date(2016,2,1) "MIT Medical" "Adult well visit" false 100]
# claim2 = [Date(2016,4,1) "Harvard Vanguard" "Urgent care" false 100]
# claim3 = [Date(2016,9,1) "Great Hill Dental" "Annual dental visit" false 200]
# claims = [claim1; claim2; claim3]

###### Starting claim acceptance model
# accept = true

# If claim type not covered by plan level, reject
# Need to compile a document that lists possible claims and what is covered by what level

# Reject if not pre-authorized for CT, MRI

# Reject if out-of-network provider

# Reject if more claims in the last 3 months than previous 12

# Reject if more cost in last 3 months than previous 12

# Reject if two claims in a red zone category

# Reject if two months or more late on premium payments


end # module
