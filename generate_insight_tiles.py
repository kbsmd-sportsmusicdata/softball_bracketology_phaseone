
import pandas as pd

def generate_insight_tiles(stats_df, model_probs_df, output_path):
    insights = []

    for idx, row in stats_df.iterrows():
        team = row['Team']
        conference = row['Conference']
        era_rank_conf = row.get('ERA_Conf_Rank')
        era_rank_nat = row.get('ERA_Nat_Rank')
        ba_rank_nat = row.get('BA_Nat_Rank')
        sbg_rank_nat = row.get('SBG_Nat_Rank')
        fpct_rank_conf = row.get('FPCT_Conf_Rank')
        fpct_rank_nat = row.get('FPCT_Nat_Rank')
        doubles_rank_conf = row.get('Doubles_Conf_Rank')
        model_prob = model_probs_df.loc[model_probs_df['Team'] == team, 'WCWS_Prob'].values[0]

        if era_rank_conf == 1 and era_rank_nat == 1:
            insights.append({
                "Insight_Title": f"{team}'s Pitching Supremacy",
                "Insight_Text": f"{team} leads both the {conference} and nation in ERA — setting the standard defensively for 2025.",
                "Team": team,
                "Conference": conference,
                "Insight_Type": "Pitching Dominance",
                "Stat_Category": "ERA",
                "Highlight_Rank": "1st Conf, 1st Nat in ERA",
                "Weighted_Probability_Impact": round(model_prob, 2)
            })

        if fpct_rank_conf <= 3 and era_rank_conf <= 3:
            insights.append({
                "Insight_Title": f"{team}'s Defensive Fortress",
                "Insight_Text": f"{team} ranks top 3 in both ERA and fielding percentage in the {conference}.",
                "Team": team,
                "Conference": conference,
                "Insight_Type": "Defensive Team",
                "Stat_Category": "ERA, FPCT",
                "Highlight_Rank": "Top 3 Conf ERA + FPCT",
                "Weighted_Probability_Impact": round(model_prob * 0.9, 2)
            })

        if ba_rank_nat and ba_rank_nat <= 5 and doubles_rank_conf == 1:
            insights.append({
                "Insight_Title": f"{team}'s Offensive Surge",
                "Insight_Text": f"{team} leads the {conference} in doubles and ranks top 5 nationally in batting average.",
                "Team": team,
                "Conference": conference,
                "Insight_Type": "Offensive Power",
                "Stat_Category": "Doubles, BA",
                "Highlight_Rank": "1st Conf Doubles, Top 5 BA Nat",
                "Weighted_Probability_Impact": round(model_prob * 0.95, 2)
            })

        if sbg_rank_nat and sbg_rank_nat <= 5:
            insights.append({
                "Insight_Title": f"{team}'s Speed Factor",
                "Insight_Text": f"{team} ranks top 5 nationally in stolen bases per game — a key disruptor in tight matchups.",
                "Team": team,
                "Conference": conference,
                "Insight_Type": "Baserunning Threat",
                "Stat_Category": "SB/G",
                "Highlight_Rank": "Top 5 Nat in SB/G",
                "Weighted_Probability_Impact": round(model_prob * 0.88, 2)
            })

    insights_df = pd.DataFrame(insights)
    insights_df.to_csv(output_path, index=False)
    return insights_df
