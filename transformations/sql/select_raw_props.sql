with 
	overs as (
		select
			participant_name ,
			subcategory_name ,
			outcome_label ,
			outcome_line ,
			outcome_oddsAmerican::int as over_odds ,
			timestamp
		from main.fact_dk_offers 
		where 
			outcome_label = 'Over'
			/* fantasy-relevant stats categories */
			and subcategory_name in (
				'FG Made', 'Interceptions O/U', 'Pass TDs O/U', 'Pass Yards O/U', 'Rec Yards O/U',
				'Receptions O/U', 'Rush + Rec Yards O/U', 'Rush Yards O/U', 'PAT Made'
			)
			/* scraped today */
			and date_trunc('day', timestamp) = (select date_trunc('day', max(timestamp)) from main.fact_dk_offers)
	),
	unders as (
		select
			participant_name ,
			subcategory_name ,
			outcome_label ,
			outcome_line ,
			outcome_oddsAmerican::int as under_odds ,
			timestamp
		from main.fact_dk_offers 
		where 
			outcome_label = 'Under'
			and subcategory_name in (
				'FG Made', 'Interceptions O/U', 'Pass TDs O/U', 'Pass Yards O/U', 'Rec Yards O/U',
				'Receptions O/U', 'Rush + Rec Yards O/U', 'Rush Yards O/U', 'PAT Made'
			)
			and date_trunc('day', timestamp) = (select date_trunc('day', max(timestamp)) from main.fact_dk_offers)
	),
	tds as (
		select
			participant_name ,
			subcategory_name ,
			0.5 as outcome_line ,
			outcome_oddsAmerican::int as over_odds ,
			outcome_oddsAmerican::int * -1 as under_odds ,
			timestamp
		from main.fact_dk_offers 
		where 
			subcategory_name = 'TD Scorer'
			and offer_label = 'Anytime TD Scorer'
			and date_trunc('day', timestamp) = (select date_trunc('day', max(timestamp)) from main.fact_dk_offers)
	)
select
	o.participant_name ,
	o.subcategory_name ,
	o.outcome_line ,
	o.over_odds ,
	u.under_odds ,
	o.timestamp 
from overs o
join unders u
	on 
		o.participant_name = u.participant_name
		and o.subcategory_name = u.subcategory_name
union
select * from tds
order by participant_name, timestamp desc;