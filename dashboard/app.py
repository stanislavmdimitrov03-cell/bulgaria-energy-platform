
import streamlit as st
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import os
from pyathena import connect as athena_connect
from pyathena.pandas.cursor import PandasCursor



st.set_page_config(
    page_title="Bulgaria Energy Intelligence",
    layout="wide"
)


# DATABASE CONNECTION


USE_CLOUD = True  # set to True to use Athena, False to use local DuckDB

@st.cache_resource
def get_local_connection():
    """Connect to local DuckDB database."""
    db_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'bulgaria_energy.db')
    )
    con = duckdb.connect(db_path, read_only=True)
    con.execute("SET temp_directory='C:/Users/stann/Projects/bulgaria-energy-platform/tmp'")
    return con

@st.cache_resource
def get_cloud_connection():
    """Connect to Athena (cloud data lake)."""
    return athena_connect(
        s3_staging_dir="s3://bulgaria-energy-platform-sd-2024/athena-results/",
        region_name="eu-central-1",
        schema_name="bulgaria_energy",
        cursor_class=PandasCursor
    )

def run_query(sql):

    if USE_CLOUD:
        con = get_cloud_connection()
        cursor = con.cursor()
        return cursor.execute(sql).as_pandas()
    else:
        con = get_local_connection()
        return con.execute(sql).df()

# SIDEBAR NAVIGATION


st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select a page:",
    ["Energy Mix Overview", "Price Analysis", "Weather and Renewables", "Key Insights"]
)

# PAGE 1: ENERGY MIX OVERVIEW


if page == "Energy Mix Overview":

    st.title("Bulgaria Energy Mix 2024")
    st.markdown("How Bulgaria generated its electricity throughout 2024.")

    # KPI Cards

    col1, col2, col3, col4 = st.columns(4)

    # Pull summary stats from DuckDB
    stats = run_query("""
        SELECT
            ROUND(AVG(renewable_pct), 1)      AS avg_renewable_pct,
            ROUND(AVG(nuclear_mw), 0)          AS avg_nuclear_mw,
            ROUND(AVG(total_generation_mw), 0) AS avg_total_mw,
            ROUND(AVG(solar_mw), 0)            AS avg_solar_mw
        FROM fct_generation_hourly
    """)


    col1.metric("Avg Renewable Share", f"{stats['avg_renewable_pct'][0]}%")
    col2.metric("Avg Nuclear Output", f"{int(stats['avg_nuclear_mw'][0])} MW")
    col3.metric("Avg Total Generation", f"{int(stats['avg_total_mw'][0])} MW")
    col4.metric("Avg Solar Output", f"{int(stats['avg_solar_mw'][0])} MW")

    st.divider()

    # Monthly Energy Mix Chart
    st.subheader("Monthly Generation by Source")

    monthly_mix = run_query("""
        SELECT
            d.month,
            d.month_name,
            ROUND(AVG(g.nuclear_mw), 1)      AS nuclear_mw,
            ROUND(AVG(g.lignite_mw), 1)       AS lignite_mw,
            ROUND(AVG(g.solar_mw), 1)         AS solar_mw,
            ROUND(AVG(g.wind_onshore_mw), 1)  AS wind_mw,
            ROUND(AVG(g.hydro_reservoir_mw +
                      g.hydro_river_mw), 1)   AS hydro_mw,
            ROUND(AVG(g.gas_mw), 1)           AS gas_mw,
            ROUND(AVG(g.renewable_pct), 1)    AS renewable_pct
        FROM fct_generation_hourly g
        JOIN dim_date d ON g.date_key = d.date_key
        GROUP BY d.month, d.month_name
        ORDER BY d.month
    """)

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.stackplot(
        monthly_mix['month_name'],
        monthly_mix['nuclear_mw'],
        monthly_mix['lignite_mw'],
        monthly_mix['solar_mw'],
        monthly_mix['wind_mw'],
        monthly_mix['hydro_mw'],
        monthly_mix['gas_mw'],
        labels=['Nuclear', 'Lignite', 'Solar', 'Wind', 'Hydro', 'Gas'],
        colors=['#c0392b', '#2c3e50', '#f1c40f', '#27ae60', '#2980b9', '#e67e22']
    )
    ax.set_xlabel("Month")
    ax.set_ylabel("Average Generation (MW)")
    ax.legend(loc='upper left')
    plt.xticks(rotation=45)
    plt.tight_layout()

    st.pyplot(fig)

    st.divider()

   # Renewable Share Line Chart
    st.subheader("Monthly Renewable Share")

    fig2, ax2 = plt.subplots(figsize=(14, 4))
    ax2.plot(
        monthly_mix['month_name'],
        monthly_mix['renewable_pct'],
        color='#27ae60', linewidth=2.5, marker='o'
    )
    ax2.fill_between(
        monthly_mix['month_name'],
        monthly_mix['renewable_pct'],
        alpha=0.2, color='#27ae60'
    )
    ax2.set_xlabel("Month")
    ax2.set_ylabel("Renewable Share (%)")
    ax2.set_ylim(0, 60)
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig2)

    # Raw data toggle
    if st.checkbox("Show raw data"):
        st.dataframe(monthly_mix)


# PLACEHOLDER PAGES

elif page == "Price Analysis":

    st.title("Electricity Price Analysis 2024")
    st.markdown("Day-ahead wholesale electricity prices for Bulgaria.")

    #  KPI Cards
    col1, col2, col3, col4 = st.columns(4)

    price_stats = run_query("""
        SELECT
            ROUND(AVG(price_eur_mwh), 2)  AS avg_price,
            ROUND(MIN(price_eur_mwh), 2)  AS min_price,
            ROUND(MAX(price_eur_mwh), 2)  AS max_price,
            COUNT(CASE WHEN is_negative_price THEN 1 END) AS negative_hours
        FROM fct_prices_hourly
    """)

    col1.metric("Average Price", f"{price_stats['avg_price'][0]} EUR/MWh")
    col2.metric("Minimum Price", f"{price_stats['min_price'][0]} EUR/MWh")
    col3.metric("Maximum Price", f"{price_stats['max_price'][0]} EUR/MWh")
    col4.metric("Negative Price Hours", f"{int(price_stats['negative_hours'][0])}")

    st.divider()

    # Price over time
    st.subheader("Monthly Average Price")

    monthly_prices = run_query("""
        SELECT
            d.month,
            d.month_name,
            ROUND(AVG(p.price_eur_mwh), 2) AS avg_price,
            ROUND(MIN(p.price_eur_mwh), 2)  AS min_price,
            ROUND(MAX(p.price_eur_mwh), 2)  AS max_price
        FROM fct_prices_hourly p
        JOIN dim_date d ON p.date_key = d.date_key
        GROUP BY d.month, d.month_name
        ORDER BY d.month
    """)

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.fill_between(
        monthly_prices['month_name'],
        monthly_prices['min_price'].clip(lower=0),
        monthly_prices['max_price'],
        alpha=0.15, color='steelblue', label='Min-Max range'
    )
    ax.plot(
        monthly_prices['month_name'],
        monthly_prices['avg_price'],
        color='steelblue', linewidth=2.5,
        marker='o', label='Average price'
    )
    ax.set_xlabel("Month")
    ax.set_ylabel("Price (EUR/MWh)")
    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig)

    st.divider()

    # Price by hour heatmap
    st.subheader("Price Heatmap by Hour and Season")

    heatmap_data = run_query("""
        SELECT
            d.season,
            d.hour,
            ROUND(AVG(p.price_eur_mwh), 2) AS avg_price
        FROM fct_prices_hourly p
        JOIN dim_date d ON p.date_key = d.date_key
        GROUP BY d.season, d.hour
        ORDER BY d.hour
    """)

    heatmap_pivot = heatmap_data.pivot(
        index='season', columns='hour', values='avg_price'
    )
    season_order = ['Winter', 'Spring', 'Summer', 'Autumn']
    heatmap_pivot = heatmap_pivot.reindex(season_order)

    fig2, ax2 = plt.subplots(figsize=(14, 4))
    im = ax2.imshow(heatmap_pivot.values, aspect='auto', cmap='RdYlGn_r')
    ax2.set_xlabel("Hour of Day")
    ax2.set_ylabel("Season")
    ax2.set_xticks(range(24))
    ax2.set_xticklabels(range(24))
    ax2.set_yticks(range(4))
    ax2.set_yticklabels(season_order)
    plt.colorbar(im, ax=ax2, label='EUR/MWh')
    plt.tight_layout()
    st.pyplot(fig2)

    st.divider()

    # Price vs renewable scatter
    st.subheader("Price vs Renewable Share")

    scatter_data = run_query("""
        SELECT
            g.renewable_pct,
            p.price_eur_mwh
        FROM fct_generation_hourly g
        JOIN fct_prices_hourly p ON g.date_key = p.date_key
    """)

    fig3, ax3 = plt.subplots(figsize=(10, 5))
    ax3.scatter(
        scatter_data['renewable_pct'],
        scatter_data['price_eur_mwh'],
        alpha=0.1, color='steelblue', s=5
    )
    ax3.axhline(y=0, color='red', linestyle='--', linewidth=0.8)
    ax3.set_xlabel("Renewable Share (%)")
    ax3.set_ylabel("Price (EUR/MWh)")
    plt.tight_layout()
    st.pyplot(fig3)

    correlation = scatter_data['renewable_pct'].corr(scatter_data['price_eur_mwh'])
    st.info(f"Correlation between renewable share and price: {correlation:.3f} — higher renewables tend to lower prices")

    if st.checkbox("Show raw monthly price data"):
        st.dataframe(monthly_prices)

elif page == "Weather and Renewables":

    st.title("Weather and Renewable Generation 2024")
    st.markdown("How solar radiation and wind speed relate to renewable electricity generation.")

    st.divider()

    # Solar radiation vs solar generation
    st.subheader("Solar Radiation vs Solar Generation")

    # City selector — user can pick which city's weather to display
    city = st.selectbox("Select city for weather data:",
                        ["Sofia", "Plovdiv", "Varna", "Burgas", "Pleven"])

    # Get the city_key for the selected city
    city_key = get_local_connection().execute(f"""
        SELECT city_key FROM dim_city WHERE city = '{city}'
    """).fetchone()[0]

    solar_data = run_query(f"""
        SELECT
            w.shortwave_radiation_wm2,
            g.solar_mw,
            d.month
        FROM fct_weather_hourly w
        JOIN fct_generation_hourly g ON w.date_key = g.date_key
        JOIN dim_date d              ON w.date_key = d.date_key
        WHERE w.city_key = {city_key}
        AND w.shortwave_radiation_wm2 > 0
    """)

    fig, ax = plt.subplots(figsize=(12, 5))
    scatter = ax.scatter(
        solar_data['shortwave_radiation_wm2'],
        solar_data['solar_mw'],
        c=solar_data['month'],
        cmap='RdYlGn',
        alpha=0.3, s=5
    )
    plt.colorbar(scatter, ax=ax, label='Month')
    ax.set_xlabel(f"Solar Radiation in {city} (W/m2)")
    ax.set_ylabel("Solar Generation in Bulgaria (MW)")
    plt.tight_layout()
    st.pyplot(fig)

    corr = solar_data['shortwave_radiation_wm2'].corr(solar_data['solar_mw'])
    st.info(f"Correlation between solar radiation and solar generation: {corr:.3f}")

    st.divider()

    # Monthly solar radiation by city
    st.subheader("Monthly Solar Radiation Across Cities")

    city_solar = run_query("""
        SELECT
            c.city,
            d.month,
            d.month_name,
            ROUND(AVG(w.shortwave_radiation_wm2), 1) AS avg_radiation
        FROM fct_weather_hourly w
        JOIN dim_city c ON w.city_key = c.city_key
        JOIN dim_date d ON w.date_key = d.date_key
        GROUP BY c.city, d.month, d.month_name
        ORDER BY c.city, d.month
    """)

    fig2, ax2 = plt.subplots(figsize=(14, 5))
    colors_city = {
        'Sofia': '#2980b9',
        'Plovdiv': '#27ae60',
        'Varna': '#c0392b',
        'Burgas': '#e67e22',
        'Pleven': '#8e44ad'
    }
    for city_name, group in city_solar.groupby('city'):
        ax2.plot(
            group['month_name'],
            group['avg_radiation'],
            marker='o', linewidth=2,
            label=city_name,
            color=colors_city[city_name]
        )
    ax2.set_xlabel("Month")
    ax2.set_ylabel("Average Solar Radiation (W/m2)")
    ax2.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig2)

    st.divider()

    # Wind speed vs wind generation
    st.subheader("Wind Speed vs Wind Generation")

    wind_data = run_query(f"""
        SELECT
            w.windspeed_10m_kmh,
            g.wind_onshore_mw,
            d.season
        FROM fct_weather_hourly w
        JOIN fct_generation_hourly g ON w.date_key = g.date_key
        JOIN dim_date d              ON w.date_key = d.date_key
        WHERE w.city_key = {city_key}
    """)

    fig3, ax3 = plt.subplots(figsize=(12, 5))
    colors_season = {
        'Winter': '#2980b9',
        'Spring': '#27ae60',
        'Summer': '#e67e22',
        'Autumn': '#c0392b'
    }
    for season, group in wind_data.groupby('season'):
        ax3.scatter(
            group['windspeed_10m_kmh'],
            group['wind_onshore_mw'],
            alpha=0.2, s=5,
            color=colors_season[season],
            label=season
        )
    ax3.set_xlabel(f"Wind Speed in {city} (km/h)")
    ax3.set_ylabel("Wind Generation in Bulgaria (MW)")
    ax3.legend()
    plt.tight_layout()
    st.pyplot(fig3)

    corr_wind = wind_data['windspeed_10m_kmh'].corr(wind_data['wind_onshore_mw'])
    st.info(f"Correlation between wind speed and wind generation: {corr_wind:.3f}")

elif page == "Key Insights":

    st.title("Key Insights")
    st.markdown("Summary of the most important findings from the analysis.")

    st.divider()

    #Summary statistics
    st.subheader("2024 at a Glance")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**Generation**")
        gen_stats = run_query("""
            SELECT
                ROUND(AVG(total_generation_mw), 0) AS avg_total,
                ROUND(AVG(renewable_pct), 1)        AS avg_renewable,
                ROUND(AVG(nuclear_mw), 0)           AS avg_nuclear,
                ROUND(AVG(solar_mw), 0)             AS avg_solar
            FROM fct_generation_hourly
        """)
        st.metric("Avg Total Generation", f"{int(gen_stats['avg_total'][0])} MW")
        st.metric("Avg Renewable Share", f"{gen_stats['avg_renewable'][0]}%")
        st.metric("Avg Nuclear Output", f"{int(gen_stats['avg_nuclear'][0])} MW")
        st.metric("Avg Solar Output", f"{int(gen_stats['avg_solar'][0])} MW")

    with col2:
        st.markdown("**Prices**")
        price_stats = run_query("""
            SELECT
                ROUND(AVG(price_eur_mwh), 2)  AS avg_price,
                ROUND(MIN(price_eur_mwh), 2)  AS min_price,
                ROUND(MAX(price_eur_mwh), 2)  AS max_price,
                COUNT(CASE WHEN is_negative_price 
                      THEN 1 END)             AS negative_hours
            FROM fct_prices_hourly
        """)
        st.metric("Average Price", f"{price_stats['avg_price'][0]} EUR/MWh")
        st.metric("Minimum Price", f"{price_stats['min_price'][0]} EUR/MWh")
        st.metric("Maximum Price", f"{price_stats['max_price'][0]} EUR/MWh")
        st.metric("Negative Price Hours", f"{int(price_stats['negative_hours'][0])}")

    with col3:
        st.markdown("**Weather**")
        weather_stats = run_query("""
       SELECT ROUND(AVG(shortwave_radiation_wm2), 1) AS avg_radiation,
           ROUND(MAX(shortwave_radiation_wm2), 1) AS max_radiation,
        ROUND(AVG(temperature_2m_c), 1)        AS avg_temp,
           ROUND(AVG(windspeed_10m_kmh), 1)       AS avg_wind
             FROM fct_weather_hourly
                    WHERE city_key = 4
                       """)
        st.metric("Avg Solar Radiation",
                  f"{weather_stats['avg_radiation'][0]} W/m2")
        st.metric("Peak Solar Radiation",
                  f"{weather_stats['max_radiation'][0]} W/m2")
        st.metric("Avg Temperature",
                  f"{weather_stats['avg_temp'][0]} C")
        st.metric("Avg Wind Speed",
                  f"{weather_stats['avg_wind'][0]} km/h")

    st.divider()

    # Key findings
    st.subheader("Key Findings")

    findings = [
        ("Solar drives midday price drops",
         "The cheapest hour of the day is 13:00 (61.59 EUR/MWh) — exactly when "
         "solar generation peaks. As Bulgaria adds more solar capacity this "
         "midday price dip will deepen further."),

        ("Evening prices are the most expensive",
         "20:00 is the most expensive hour on average (195.32 EUR/MWh). "
         "Solar generation drops to zero at sunset while demand remains high "
         "from households returning home."),

        ("Strong solar radiation to generation link",
         "Correlation of 0.865 between solar radiation and solar generation "
         "confirms the data is physically accurate and the relationship is "
         "direct and strong."),

        ("Renewables do reduce prices",
         "Correlation of -0.142 between renewable share and price. "
         "When renewable share exceeds 50%, average prices drop to "
         "around 62 EUR/MWh compared to 99 EUR/MWh at near-zero renewable share."),

        ("Nuclear is Bulgaria's backbone",
         "Nuclear generation from Kozloduy averages 1,766 MW — "
         "43.1% of total generation. It runs at constant output 24 hours "
         "a day regardless of weather or demand."),

        ("55 hours of negative prices in 2024",
         "Bulgaria experienced 55 hours of negative electricity prices — "
         "0.6% of all hours. These occur when solar and wind generation "
         "exceeds demand and grid operators pay consumers to use electricity."),

        ("Spring has the cheapest electricity",
         "Average spring price is 68.04 EUR/MWh — the cheapest season. "
         "Mild temperatures reduce heating and cooling demand while "
         "solar generation is already strong.")
    ]

    for title, description in findings:
        with st.expander(title):
            st.write(description)

    st.divider()

    # Seasonal summary table
    st.subheader("Seasonal Summary")

    seasonal = run_query("""
        SELECT
            d.season                              AS Season,
            ROUND(AVG(g.renewable_pct), 1)        AS "Renewable %",
            ROUND(AVG(g.solar_mw), 0)             AS "Avg Solar MW",
            ROUND(AVG(g.wind_onshore_mw), 0)      AS "Avg Wind MW",
            ROUND(AVG(p.price_eur_mwh), 2)        AS "Avg Price EUR/MWh",
            COUNT(CASE WHEN p.is_negative_price 
                  THEN 1 END)                     AS "Negative Price Hours"
        FROM fct_generation_hourly g
        JOIN fct_prices_hourly p ON g.date_key = p.date_key
        JOIN dim_date d          ON g.date_key = d.date_key
        GROUP BY d.season
        ORDER BY "Renewable %" DESC
    """)

    st.dataframe(seasonal, use_container_width=True)

