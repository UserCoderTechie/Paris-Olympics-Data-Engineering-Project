import os
import pandas as pd
import matplotlib.pyplot as plt

# -------------------------------
# Setup paths
# -------------------------------
GOLD_PATH = "/workspaces/Paris-Olympics-Data-Engineering-Project/gold"
PLOT_PATH = "/workspaces/Paris-Olympics-Data-Engineering-Project/plots"

os.makedirs(PLOT_PATH, exist_ok=True)


# -------------------------------
# Helper: Load Parquet as Pandas
# -------------------------------
def load_parquet(name):
    path = f"{GOLD_PATH}/{name}"
    return pd.read_parquet(path)


# -------------------------------
# 1. Top 10 Athlete Countries
# -------------------------------
def plot_top_athlete_countries():
    df = load_parquet("athletes_per_country")
    df = df.sort_values("total_athletes", ascending=False).head(10)

    df.plot(x="country", y="total_athletes", kind="bar")
    plt.title("Top 10 Countries by Number of Athletes")
    plt.tight_layout()
    plt.savefig(f"{PLOT_PATH}/top_athletes.png")
    plt.close()


# -------------------------------
# 2. Top 10 Coach Countries
# -------------------------------
def plot_top_coach_countries():
    df = load_parquet("coaches_per_country")
    df = df.sort_values("total_coaches", ascending=False).head(10)

    df.plot(x="country", y="total_coaches", kind="bar")
    plt.title("Top 10 Countries by Number of Coaches")
    plt.tight_layout()
    plt.savefig(f"{PLOT_PATH}/top_coaches.png")
    plt.close()


# -------------------------------
# 3. Events per Sport
# -------------------------------
def plot_events_per_sport():
    df = load_parquet("events_per_sport")
    df = df.sort_values("event_count", ascending=False)

    df.plot(x="sport", y="event_count", kind="bar")
    plt.title("Events Per Sport")
    plt.tight_layout()
    plt.savefig(f"{PLOT_PATH}/events_per_sport.png")
    plt.close()


# -------------------------------
# 4. Scatter: Athletes vs Coaches
# -------------------------------
def plot_athletes_vs_coaches():
    athletes = load_parquet("athletes_per_country")
    coaches = load_parquet("coaches_per_country")

    merged = pd.merge(athletes, coaches, on="country", how="inner")

    plt.scatter(merged["total_athletes"], merged["total_coaches"])
    plt.xlabel("Total Athletes")
    plt.ylabel("Total Coaches")
    plt.title("Athletes vs Coaches by Country")
    plt.tight_layout()
    plt.savefig(f"{PLOT_PATH}/athletes_vs_coaches.png")
    plt.close()


# -------------------------------
# 5. Bar Chart — Top 15 Sports
# -------------------------------
def plot_top_sports():
    df = load_parquet("events_per_sport")
    df = df.sort_values("event_count", ascending=False).head(15)

    df.plot(x="sport", y="event_count", kind="bar")
    plt.title("Top 15 Sports by Event Count")
    plt.tight_layout()
    plt.savefig(f"{PLOT_PATH}/top_sports.png")
    plt.close()


# -------------------------------
# 6. Pie Chart — Top 5 Athlete Countries
# -------------------------------
def plot_athlete_distribution_pie():
    df = load_parquet("athletes_per_country")
    df = df.sort_values("total_athletes", ascending=False).head(5)

    plt.pie(df["total_athletes"], labels=df["country"], autopct="%1.1f%%")
    plt.title("Top 5 Athlete Countries - Distribution")
    plt.tight_layout()
    plt.savefig(f"{PLOT_PATH}/athlete_pie.png")
    plt.close()


# -------------------------------
# 7. Combined Athletes & Coaches (Side-by-Side)
# -------------------------------
def plot_combined_bar():
    athletes = load_parquet("athletes_per_country")
    coaches = load_parquet("coaches_per_country")

    merged = pd.merge(athletes, coaches, on="country")
    merged = merged.sort_values("total_athletes", ascending=False).head(10)

    merged.plot(x="country", y=["total_athletes", "total_coaches"], kind="bar")
    plt.title("Athletes & Coaches by Country (Top 10)")
    plt.tight_layout()
    plt.savefig(f"{PLOT_PATH}/combined_bars.png")
    plt.close()


# -------------------------------
# MAIN
# -------------------------------
def main():
    print("\n Generating Olympic Visualisation PNGs...\n")

    plot_top_athlete_countries()
    plot_top_coach_countries()
    plot_events_per_sport()
    plot_athletes_vs_coaches()
    plot_top_sports()
    plot_athlete_distribution_pie()
    plot_combined_bar()

    print("\n All 7 visualisations saved successfully in /plots \n")


if __name__ == "__main__":
    main()
