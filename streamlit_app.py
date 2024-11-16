import streamlit as st
import pandas as pd
from pinotdb import connect
import matplotlib.pyplot as plt
import seaborn as sns

# Set page configuration
st.set_page_config(page_title="Jirawat Realtime Dashboard", layout="wide")

# Dashboard Title
st.title("Thanakrit Realtime Dashboard")
st.write("Real-time Pageview and User Activity Insights")

# Function for connecting to Pinot
def create_connection():
    conn = connect(host='18.143.172.71', port=8099, path='/query/sql', scheme='http')
    return conn

# Establish connection to Pinot
conn = create_connection()
curs1 = conn.cursor()
curs2 = conn.cursor()
curs3 = conn.cursor()
curs4 = conn.cursor()

# --------------------- Filters ---------------------
# Viewtime Range Filter for Query 1
viewtime_options = [
    'Range 1 (1-100)', 'Range 2 (101-200)', 'Range 3 (201-300)',
    'Range 4 (301-400)', 'Range 5 (401-500)', 'Range 6 (501-600)',
    'Range 7 (601-700)', 'Range 8+ (701 and above)'
]
viewtime_filter = st.multiselect('Select Viewtime Range', viewtime_options, default=viewtime_options)

# Day of the Week Filter for Query 4
day_filter = st.multiselect('Select Days of the Week', ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'], default=None)

# --------------------- Queries with Filters ---------------------

# Query 1 - View Count by Viewtime Range
query1 = f"""
SELECT
  CASE
    WHEN viewtime BETWEEN 1 AND 100 THEN 'Range 1 (1-100)'
    WHEN viewtime BETWEEN 101 AND 200 THEN 'Range 2 (101-200)'
    WHEN viewtime BETWEEN 201 AND 300 THEN 'Range 3 (201-300)'
    WHEN viewtime BETWEEN 301 AND 400 THEN 'Range 4 (301-400)'
    WHEN viewtime BETWEEN 401 AND 500 THEN 'Range 5 (401-500)'
    WHEN viewtime BETWEEN 501 AND 600 THEN 'Range 6 (501-600)'
    WHEN viewtime BETWEEN 601 AND 700 THEN 'Range 7 (601-700)'
    ELSE 'Range 8+ (701 and above)'
  END AS viewtime_range,
  COUNT(*) AS view_count
FROM
  pageviews_stream_REALTIME
WHERE
  CASE
    WHEN viewtime BETWEEN 1 AND 100 THEN 'Range 1 (1-100)'
    WHEN viewtime BETWEEN 101 AND 200 THEN 'Range 2 (101-200)'
    WHEN viewtime BETWEEN 201 AND 300 THEN 'Range 3 (201-300)'
    WHEN viewtime BETWEEN 301 AND 400 THEN 'Range 4 (301-400)'
    WHEN viewtime BETWEEN 401 AND 500 THEN 'Range 5 (401-500)'
    WHEN viewtime BETWEEN 501 AND 600 THEN 'Range 6 (501-600)'
    WHEN viewtime BETWEEN 601 AND 700 THEN 'Range 7 (601-700)'
    ELSE 'Range 8+ (701 and above)'
  END IN ({', '.join([f"'{x}'" for x in viewtime_filter])})
GROUP BY
  viewtime_range
ORDER BY
  view_count DESC;
"""
curs1.execute(query1)
result1 = curs1.fetchall()
df1 = pd.DataFrame(result1, columns=['viewtime_range', 'view_count'])
df1_filtered = df1[df1['viewtime_range'].isin(viewtime_filter)]

# Query 2 - User Count by City and Level
query2 = """
SELECT
  city,
  level,
  COUNT(*) AS user_count
FROM
  users_clickstream_REALTIME
GROUP BY
  city, level
ORDER BY
  city, level;
"""
curs2.execute(query2)
result2 = curs2.fetchall()
df2 = pd.DataFrame(result2, columns=['city', 'level', 'user_count'])

# Query 3 - User Count by Region and Gender
query3 = """
SELECT
  regionid,
  gender,
  COUNT(*) AS user_count
FROM
  users_table_REALTIME
GROUP BY
  regionid, gender
ORDER BY
  user_count DESC;
"""
curs3.execute(query3)
result3 = curs3.fetchall()
df3 = pd.DataFrame(result3, columns=['regionid', 'gender', 'user_count'])

# Query 4 - Registration Count by Day of the Week
query4 = f"""
SELECT 
    CASE 
        WHEN MOD(CAST(registertime / 86400000 AS INT), 7) = 0 THEN 'Sun'
        WHEN MOD(CAST(registertime / 86400000 AS INT), 7) = 1 THEN 'Mon'
        WHEN MOD(CAST(registertime / 86400000 AS INT), 7) = 2 THEN 'Tue'
        WHEN MOD(CAST(registertime / 86400000 AS INT), 7) = 3 THEN 'Wed'
        WHEN MOD(CAST(registertime / 86400000 AS INT), 7) = 4 THEN 'Thu'
        WHEN MOD(CAST(registertime / 86400000 AS INT), 7) = 5 THEN 'Fri'
        WHEN MOD(CAST(registertime / 86400000 AS INT), 7) = 6 THEN 'Sat'
    END AS day_of_week,
    COUNT(userid) AS registration_count
FROM 
    users_table_REALTIME
GROUP BY 
    day_of_week
ORDER BY 
    day_of_week;
"""
curs4.execute(query4)
result4 = curs4.fetchall()
df4 = pd.DataFrame(result4, columns=['day_of_week', 'registration_count'])
df4_filtered = df4[df4['day_of_week'].isin(day_filter)] if day_filter else df4


# ---------------------------------------------------
# Display Visualizations in Four Columns
col1, col2, col3, col4 = st.columns(4)

# Column 1: View Count by Viewtime Range
with col1:
    fig1, ax1 = plt.subplots(figsize=(4, 3))  # Smaller figure size
    sns.barplot(data=df1_filtered, x='viewtime_range', y='view_count', ax=ax1, color="skyblue")
    ax1.set_title('View Count by Viewtime Range')
    ax1.set_xlabel('Viewtime Range')
    ax1.set_ylabel('View Count')
    plt.xticks(rotation=45)
    st.pyplot(fig1)

# Column 2: User Count by City and Level
with col2:
    fig2, ax2 = plt.subplots(figsize=(4, 3))  # Smaller figure size
    sns.barplot(data=df2, x='city', y='user_count', hue='level', ax=ax2)
    ax2.set_title('User Count by City and Level')
    ax2.set_xlabel('City')
    ax2.set_ylabel('User Count')
    plt.xticks(rotation=45)
    st.pyplot(fig2)

# Column 3: Gender Distribution Across All Regions
with col3:
    fig3, ax3 = plt.subplots(figsize=(4, 3))  # Smaller figure size
    sns.barplot(data=df3, x='regionid', y='user_count', hue='gender', ax=ax3)
    ax3.set_title('Gender Distribution Across All Regions')
    ax3.set_xlabel('Region ID')
    ax3.set_ylabel('User Count')
    plt.xticks(rotation=45)
    st.pyplot(fig3)

# Column 4: User Registration Distribution by Day of the Week
with col4:
    fig4, ax4 = plt.subplots(figsize=(4, 3))  # Smaller figure size
    sns.barplot(data=df4_filtered, x='day_of_week', y='registration_count', palette='viridis', ax=ax4)
    ax4.set_title('User Registration Distribution by Day of the Week')
    ax4.set_xlabel("Day of the Week")
    ax4.set_ylabel("Registration Count")
    plt.xticks(rotation=0)
    st.pyplot(fig4)
