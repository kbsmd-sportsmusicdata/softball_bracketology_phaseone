
# 🥎 2025 NCAA Softball – ACC & SEC Tournament Simulation Report

## 📘 Overview
This report summarizes a multi-stage analytical model built to simulate the 2025 ACC and SEC softball conference tournaments. It includes team rankings, stat-based evaluations, and match simulations using weighted models and probability distributions.

---

## 📊 Extracted Stat Rankings (WPI Inputs)

| Conference | Team          | Category | Conf Rank | Nat Rank | Stat Highlight        | WPI Score |
|------------|---------------|----------|-----------|----------|------------------------|-----------|
| SEC        | Tennessee     | Pitching | 1         | 1        | ERA #1 (1.65)          | 1.00      |
| SEC        | LSU           | Hitting  | 1         | 7        | BA #2 (.352)           | 0.85      |
| SEC        | Arkansas      | Defense  | 1         | 1        | FPCT #1 (.985)         | 1.00      |
| Big Ten    | Indiana       | Hitting  | 1         | 1        | BA #1 (.380)           | 0.95      |
| Big Ten    | Ohio St.      | Hitting  | 1         | 1        | BA #1 (.380)           | 0.85      |
| Big Ten    | Maryland      | Defense  | 1         | 39       | DP #1 (21)             | 0.65      |
| ACC        | Florida St.   | Pitching | 1         | 9        | ERA #9 (2.19)          | 0.90      |
| ACC        | North Carolina| Hitting  | 2         | 4        | BA #3 (.353)           | 0.825     |
| ACC        | California    | Defense  | 1         | 40       | DPG #40 (0.44)         | 0.65      |

---

## ⚙️ Model Construction: WPI & Simulation Logic

### 🧮 WPI Score Formula
```
WPI = (Conference Weight + National Weight) / 2
```

#### 📏 Weight Assignments:
- **Conf Rank Weights**: 1st = 1.0, 2nd = 0.8, 3rd = 0.7, others = 0.3
- **Nat Rank Weights**: 
  - 1st = 1.0, 
  - 2nd = 0.95, 
  - 3–5 = 0.9, 
  - 6–10 = 0.85, 
  - 11–25 = 0.7, 
  - 26–50 = 0.5

---

## 📅 Matchup Simulations: ACC & SEC Tournaments

### 🔄 Championship Simulations

#### ACC Final: Florida State vs North Carolina
- **Poisson Expected Runs**: FSU 5.7 – UNC 5.0
- **Win Probabilities**: FSU 52%, UNC 48%

#### SEC Final: Oklahoma vs Texas A&M
- **Expected Runs**: OU 5.3 – A&M 4.95
- **Win Probabilities**: OU 53%, A&M 47%

---

## 📅 Day-by-Day Tournament Matchup Results

### 🟦 ACC Tournament

#### Day 1
- Louisville vs Virginia → **Virginia** (55%)
- Pittsburgh vs Stanford → **Stanford** (68%)
- Notre Dame vs North Carolina → **North Carolina** (65%)
- California vs Georgia Tech → **California** (52%)

#### Day 2
- Stanford vs Duke → **Stanford** (59%)
- Virginia vs Clemson → **Clemson** (63%)
- North Carolina vs Virginia Tech → **North Carolina** (52%)
- California vs Florida State → **Florida State** (69%)

#### Day 3
- Stanford vs North Carolina → **North Carolina** (54%)
- Clemson vs Florida State → **Florida State** (65%)

#### Championship
- Florida State vs North Carolina → **Florida State** (52%)

---

### 🟥 SEC Tournament

#### Day 1
- Auburn vs Alabama → **Alabama** (54%)
- Missouri vs Ole Miss → **Ole Miss** (60%)
- Kentucky vs Georgia → **Georgia** (51%)

#### Day 2
- Ole Miss vs Florida → **Florida** (64%)
- Georgia vs Arkansas → **Arkansas** (70%)
- Alabama vs South Carolina → **South Carolina** (55%)
- Mississippi St. vs LSU → **LSU** (58%)

#### Day 3
- Oklahoma vs LSU → **Oklahoma** (58%)
- Tennessee vs Arkansas → **Tennessee** (52%)
- Texas vs Florida → **Texas** (55%)
- Texas A&M vs South Carolina → **Texas A&M** (57%)

#### Day 4
- Oklahoma vs Tennessee → **Oklahoma** (56%)
- Texas vs Texas A&M → **Texas A&M** (51%)

#### Championship
- Oklahoma vs Texas A&M → **Oklahoma** (53%)

---

## 🧠 Key Commentary & Insights

- **Florida State** ranked #1 in conference pitching and Top 10 nationally, giving them a strong edge throughout the ACC bracket.
- **North Carolina’s** hitting made every game competitive, nearly even in expected runs against FSU.
- **Oklahoma** and **Texas A&M** dominated SEC simulations with consistently strong offensive and defensive metrics.
- The **WPI system** proved highly predictive across both conferences, reinforcing its value as a blend of performance and contextual strength.

---

## 📎 Appendix

- [CSV File: Stat Rankings & WPI Scores](./conference_stat_rankings_summary.csv)
- [CSV File: Matchup Day-by-Day Predictions](./day_by_day_matchup_predictions.csv)
- Simulation model: Poisson distribution with 15-run matrix
- Scoring assumptions: Avg RS/G and RA/G blended per matchup
