import requests
import json
import pandas as pd
import re
from dotenv import load_dotenv

load_dotenv()
import os
import MySQLdb

codes = (
    pd.read_csv(
        "https://gist.githubusercontent.com/ilyankou/b2580c632bdea4af2309dcaa69860013/raw/420fb417bcd17d833156efdf64ce8a1c3ceb2691/country-codes",
        dtype=str,
    )
    .fillna("NA")
    .set_index("ISO2")
)

# override passport-index-tidy-old.csv with passport-index-tidy.csv
# read passport-index-tidy.csv into a dataframe
# save the dataframe to passport-index-tidy-old.csv
df = pd.read_csv("passport-index-tidy.csv")
df.to_csv("passport-index-tidy-old.csv", index=False)


def fix_iso2(x):
    o = {"UK": "GB", "RK": "XK"}
    return o[x] if x in o else x


# URL of the compare passport page
url = "https://www.passportindex.org/comparebyPassport.php?p1=ro&p2=gt&p3=qa"

# Make a request to the .php page taht outputs data
result_raw = requests.post(
    "https://www.passportindex.org/incl/compare2.php",
    headers={
        "Host": "www.passportindex.org",
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Length": "9",
        "Origin": "https://www.passportindex.org",
        "DNT": "1",
        "Connection": "keep-alive",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "TE": "Trailers",
    },
    data={"compare": "1"},
)


result = json.loads(result_raw.text)
obj = {}

for passport in result:

    # Fix ISO-2 codes
    passport = fix_iso2(passport)

    # Add passport to the object
    if passport not in obj:
        obj[passport] = {}

    # Add destinations for the given passport
    for dest in result[passport]["destination"]:

        text = dest["text"]
        res = ""
        duration = dest["dur"]

        # ** Visa required, incl Cuba's tourist card **
        if text == "visa required" or text == "tourist card":
            res = "visa required"

        # ** Visa on arrival **
        elif "visa on arrival" in text:
            res = "visa on arrival"

        # ** Covid-19 ban **
        elif text == "COVID-19 ban":
            res = "covid ban"

        # ** Visa-free, incl. Seychelles' tourist registration **
        elif (
            "visa-free" in text
            or "tourist registration" in text
            or "visa waiver" in text
        ):
            res = dest["dur"] if dest["dur"] != "" else "visa free"

        # ** eVisas, incl eVisitors (Australia), eTourist cards (Suriname),
        # eTA (US), and pre-enrollment (Ivory Coast), or EVW (UK) **
        elif (
            "eVis" in text
            or "eTourist" in text
            or text == "eTA"
            or text == "pre-enrollment"
            or text == "EVW"
        ):
            res = "e-visa"

        # ** No admission, including Trump ban **
        elif text == "trump ban" or text == "not admitted":
            res = "no admission"

        # pad the duration
        if res != "" and duration != "":
            if (
                "visa-free" in text
                or "tourist registration" in text
                or "visa waiver" in text
            ):
                continue
            res = res + " (" + duration + ")"

        # Update the result!
        obj[passport][fix_iso2(dest["code"])] = res if res != "" else dest["text"]


# ISO-2: Matrix
matrix = pd.DataFrame(obj).T.fillna(-1)

# ISO-2: Tidy
matrix.stack()


# ISO-3: Matrix
iso2to3 = {x: y["ISO3"] for x, y in codes.iterrows()}


# ISO-3: Tidy
matrix.rename(columns=iso2to3, index=iso2to3).stack().to_csv(
    "passport-index-tidy-iso3.csv",
    index_label=["Passport", "Destination"],
    header=["Requirement"],
)


# Country names: Matrix
iso2name = {x: y["Country"] for x, y in codes.iterrows()}

# Country names: Tidy
matrix.rename(columns=iso2name, index=iso2name).stack().to_csv(
    "passport-index-tidy.csv",
    index_label=["Passport", "Destination"],
    header=["Requirement"],
)


# Specify the paths for your three CSV files
input_file1 = "passport-index-tidy.csv"  # Replace with your actual file path

output_file1 = "passport-index-tidy.csv"  # Replace with your actual file path


# Read the CSV file into a DataFrame
# loop through the files

df = pd.read_csv(input_file1)


# Define a function to extract numbers from brackets
def extract_duration(text):
    match = re.search(r"\((\d+)\)", str(text))
    if match:
        return int(match.group(1))
    else:
        return 0


# remove the brackets from the text
# Apply the function to create a new 'duration' column for each row
df["duration"] = df.apply(lambda row: extract_duration(row), axis=1)
df = df.assign(Requirement=df.Requirement.str.replace(r"\s*\(\d+\)", "", regex=True))
# Save the modified DataFrame to a new CSV file
# give each row a unique identifier, starting from 1
df["passport-id"] = range(1, len(df) + 1)
df.to_csv(output_file1, index=False)

# compare passport-index-tidy.csv and passport-index-tidy-old.csv
# if there are differences, use value from passport-index-tidy.csv
# put the differences in passport-index-tidy-diff.csv
# keep all columns


df1 = pd.read_csv("passport-index-tidy.csv")
df2 = pd.read_csv("passport-index-tidy-old.csv")

df1["passport-id"] = range(1, len(df1) + 1)
df2["passport-id"] = range(1, len(df2) + 1)


df_diff = (
    df1.merge(df2, indicator=True, how="outer")
    .loc[lambda x: x["_merge"] == "left_only"]
    .drop(columns=["_merge"])
)

df_diff.to_csv("passport-index-tidy-diff.csv", index=False)


connection = MySQLdb.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USERNAME"),
    passwd=os.getenv("DB_PASSWORD"),
    db=os.getenv("DB_NAME"),
    autocommit=True,
    ssl_mode="VERIFY_IDENTITY",
    ssl={"ca": "/etc/ssl/certs/ca-certificates.crt"},
)

if connection.open:
    print("Connection Successful")
    cursor = connection.cursor()

    try:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv("passport-index-tidy-diff.csv")

        # Prepare the SQL query template
        update_query = "UPDATE `passport-index` SET passport = %s, destination = %s, requirement = %s, duration = %s WHERE `passport-id` = %s"

        # Set batch size based on your preference and performance testing
        batch_size = 1000

        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i : i + batch_size]

            # Convert DataFrame rows to a list of tuples
            rows_to_update = [tuple(x) for x in batch_df.values]

            # Perform batch update
            cursor.executemany(update_query, rows_to_update)

        # Commit the transaction
        connection.commit()
        print("Update Successful")
        # print the number of rows updated
        print(f"{cursor.rowcount} rows updated")

    except Exception as e:
        # Rollback if an error occurs
        print(f"Error: {str(e)}")
        connection.rollback()

    finally:
        # Close the cursor and database connection
        cursor.close()
        connection.close()

else:
    print("Connection Failed")
