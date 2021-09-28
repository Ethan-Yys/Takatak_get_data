#coding=utf-8

# 用户性别特征
GENDER_CONF = [
    "male",
    "female",
]
FEAT_GENDER_IDX = {x: idx+1 for idx, x in enumerate(GENDER_CONF)}
VOB_BUCKET_GENDER = len(FEAT_GENDER_IDX)


# 用户年龄特征
VOB_BUCKET_AGE = 100


# 用户所属区域特征
REGION_CONF = [
    ["maharashtra", "western"],
    ["national_capital_territory_of_delhi", "northern"],
    ["uttar_pradesh", "central"],
    ["gujarat", "western"],
    ["rajasthan", "northern"],
    ["west_bengal", "eastern"],
    ["madhya_pradesh", "central"],
    ["bihar", "eastern"],
    ["karnataka", "northern"],
    ["telangana", "southern"],
    ["punjab", "northern"],
    ["haryana", "northern"],
    ["odisha", "eastern"],
    ["assam", "north_eastern"],
    ["chhattisgarh", "central"],
    ["tamil_nadu", "southern"],
    ["andhra_pradesh", "southern"],
    ["jharkhand", "eastern"],
    ["himachal_pradesh", "northern"],
    ["chandigarh", "northern"],
    ["uttarakhand", "central"],
    ["jammu_and_kashmir", "northern"],
    ["kerala", "southern"],
    ["sikkim", "north_eastern"],
    ["manipur", "north_eastern"],
    ["goa", "western"],
    ["union_territory_of_puducherry", "southern"],
    ["meghalaya", "north_eastern"],
    ["tripura", "north_eastern"],
    ["nagaland", "north_eastern"],
    ["arunachal_pradesh", "north_eastern"],
    ["daman_and_diu", "southern"],
    ["dadra_and_nagar_haveli", "western"],
    ["mizoram", "north_eastern"],
    ["andaman_and_nicobar", "southern"],
]


FEAT_STATE_IDX = {x[0]: idx+1 for idx, x in enumerate(REGION_CONF)}
VOB_BUCKET_STATE = len(FEAT_STATE_IDX)



