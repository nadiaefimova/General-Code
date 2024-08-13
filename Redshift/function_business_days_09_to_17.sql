CREATE FUNCTION f_weekday_9to17_seconds_between (timestamp without time zone, timestamp without time zone)
RETURNS numeric
STABLE
AS $$
SELECT
CAST(
CASE WHEN CAST(datediff(minutes,$1,nvl ($2,sysdate)) AS DECIMAL(38,4)) <= 0 
OR (DATE_PART(dow,$1) in (0,6) and DATE_PART(dow,nvl($2,sysdate)) in (0,6) and DATE_PART(dow,$1) = DATE_PART(dow,nvl($2,sysdate)) AND datediff ('week',$1,nvl ($2,sysdate)) =0)
    THEN CAST(0 as DECIMAL(38,4))
WHEN DATE_PART(dow,$1) NOT IN (0,6) AND DATE_PART(dow,nvl($2,sysdate)) NOT IN (0,6) AND CAST($1 AS date) = CAST(nvl($2,sysdate) AS date) THEN 
(CASE WHEN date_part(hr,$1) >= 17 OR date_part(hr,nvl($2,sysdate)) < 9 THEN 0 ELSE 
datediff (seconds,$1,nvl($2,sysdate))
--Substruct time before 9 am
- (CASE WHEN date_part(hr,$1) <9 THEN DATEDIFF(seconds,$1,dateadd(hours,9,CAST($1 AS date))) ELSE 0 END)
--Substruct time after 5 pm
- (CASE WHEN date_part(hr, nvl($2,sysdate)) >= 17 THEN DATEDIFF(seconds,dateadd(hours,17,CAST(nvl($2,sysdate) AS date)),nvl($2,sysdate)) ELSE 0 END)
END 
)
/*when the first date is weekend and the second date has less then 24 hours between (first date + next business day) and the second date */
WHEN DATE_PART(dow,$1) IN (0,6) AND DATE_PART(dow,nvl($2,sysdate)) = 1 AND (CAST($1 AS date) = CAST(dateadd(d,-1,nvl($2,sysdate)) AS date) OR CAST($1 AS date) = CAST(dateadd(d,-2,nvl($2,sysdate)) AS date)) THEN 
(CASE WHEN date_part(hr, nvl($2,sysdate)) < 9 THEN 0
	WHEN date_part(hr, nvl($2,sysdate)) >= 9 and date_part(hr, nvl($2,sysdate)) < 17 THEN DATEDIFF(seconds,dateadd(hours,9,CAST(nvl($2,sysdate) AS date)),nvl($2,sysdate))
	WHEN date_part(hr, nvl($2,sysdate)) >= 17 THEN 28800 ELSE 0 END) 
/*when the second date is weekend and the first date has less then 24 hours between first date and (second date - previous business day) */
WHEN DATE_PART(dow,nvl($2,sysdate)) IN (0,6) AND DATE_PART(dow,$1) = 5 AND (CAST($1 AS date) = CAST(dateadd(d,-1,nvl($2,sysdate)) AS date) OR CAST($1 AS date) = CAST(dateadd(d,-2,nvl($2,sysdate)) AS date)) THEN 
(CASE WHEN date_part(hr, $1) < 9 THEN 28800
	WHEN date_part(hr, $1) >= 9 and date_part(hr, $1) < 17 THEN DATEDIFF(seconds,$1,dateadd(hours,17,CAST($1 AS date)))
	ELSE 0 END) 
ELSE
(
(CAST(datediff (seconds,$1,nvl ($2,sysdate)) 
-- Subtract weekends
-(CASE WHEN datediff ('week',$1,nvl ($2,sysdate)) !=0 THEN 86400*datediff ('week',$1,nvl ($2,sysdate))*2 ELSE 0 END) 
-- Add back time if beginning on a weekend
+ CASE WHEN DATE_PART(dow,$1) IN (0,6) and datediff ('week',$1,nvl ($2,sysdate)) !=0 THEN (datediff (seconds,CAST($1 AS date),$1)) ELSE 0 END
-- Add back time if ending on a weekend
+ CASE WHEN DATE_PART(dow,nvl ($2,sysdate)) IN (0,6) and datediff ('week',$1,nvl ($2,sysdate)) !=0 THEN (86400 - datediff (seconds,CAST(nvl ($2,sysdate) AS date),nvl ($2,sysdate))) ELSE 0 END 
AS DECIMAL(38,4))
)
--Substract first day if not weekend
- (CASE WHEN DATE_PART(dow,$1) NOT IN (0,6) AND date_part(hr, $1) < 9 THEN DATEDIFF(seconds,$1,dateadd(hours, 9, CAST($1 AS date))) + 25200 -- 7 hours
WHEN DATE_PART(dow,$1) NOT IN (0,6) AND date_part(hr, $1) >= 9 and date_part(hr, $1) < 17 THEN 25200 --7 hours
WHEN DATE_PART(dow,$1) NOT IN (0,6) AND date_part(hr, $1) >= 17 THEN DATEDIFF(seconds,$1,dateadd(hours, 24, CAST($1 AS date)))
ELSE 0 END)
--Substract last day if not a weekend
- (CASE WHEN DATE_PART(dow,nvl($2,sysdate)) NOT IN (0,6) AND date_part(hr, nvl($2,sysdate)) < 9 THEN DATEDIFF(seconds,CAST(nvl($2,sysdate) AS date), nvl($2,sysdate))
WHEN DATE_PART(dow,nvl($2,sysdate)) NOT IN (0,6) AND date_part(hr, nvl($2,sysdate)) >= 9 and date_part(hr, nvl($2,sysdate)) < 17 THEN 32400 --9 hours
WHEN DATE_PART(dow,nvl($2,sysdate)) NOT IN (0,6) AND date_part(hr, nvl($2,sysdate)) >= 17 THEN 32400 + DATEDIFF(seconds,dateadd(hours,17,CAST(nvl($2,sysdate) AS date)),nvl($2,sysdate)) ELSE 0 END) 
--substract first day if it is sunday and it is the same week
- (CASE WHEN DATE_PART(dow,$1) =0 and datediff ('week',$1,nvl ($2,sysdate)) =0 THEN DATEDIFF(seconds,$1,dateadd(days, 1, CAST($1 AS date)))
ELSE 0 END)
-- Substract time for Saturday if beginning on Sunday and ending on Saturday (the same week number)
- CASE WHEN DATE_PART(dow,nvl ($2,sysdate)) =6 and datediff ('week',$1,nvl ($2,sysdate)) =0 THEN datediff (seconds,TRUNC(nvl ($2,sysdate)),nvl ($2,sysdate)) ELSE 0 END
)
-- substract not business hours from all full days
- TRUNC(((CAST(datediff (seconds,$1,nvl ($2,sysdate)) 
-- Subtract weekends
-(CASE WHEN datediff ('week',$1,nvl ($2,sysdate)) !=0 THEN 86400*datediff ('week',$1,nvl ($2,sysdate))*2 ELSE 0 END) 
-- Add back time if beginning on a weekend
+ CASE WHEN DATE_PART(dow,$1) IN (0,6) and datediff ('week',$1,nvl ($2,sysdate)) !=0 THEN (datediff (seconds,CAST($1 AS date),$1)) ELSE 0 END
-- Add back time if ending on a weekend
+ CASE WHEN DATE_PART(dow,nvl ($2,sysdate)) IN (0,6) and datediff ('week',$1,nvl ($2,sysdate)) !=0 THEN (86400 - datediff (seconds,CAST(nvl ($2,sysdate) AS date),nvl ($2,sysdate))) ELSE 0 END 
AS DECIMAL(38,4))
)
--Substract first day if not weekend
- (CASE WHEN DATE_PART(dow,$1) NOT IN (0,6) AND date_part(hr, $1) < 9 THEN DATEDIFF(seconds,$1,dateadd(hours, 9, CAST($1 AS date))) + 25200 -- 7 hours
WHEN DATE_PART(dow,$1) NOT IN (0,6) AND date_part(hr, $1) >= 9 and date_part(hr, $1) < 17 THEN 25200 --7 hours
WHEN DATE_PART(dow,$1) NOT IN (0,6) AND date_part(hr, $1) >= 17 THEN DATEDIFF(seconds,$1,dateadd(hours, 24, CAST($1 AS date)))
ELSE 0 END)
--Substract last day if not a weekend
- (CASE WHEN DATE_PART(dow,nvl($2,sysdate)) NOT IN (0,6) AND date_part(hr, nvl($2,sysdate)) < 9 THEN DATEDIFF(seconds,CAST(nvl($2,sysdate) AS date), nvl($2,sysdate))
WHEN DATE_PART(dow,nvl($2,sysdate)) NOT IN (0,6) AND date_part(hr, nvl($2,sysdate)) >= 9 and date_part(hr, nvl($2,sysdate)) < 17 THEN 32400 --9 hours
WHEN DATE_PART(dow,nvl($2,sysdate)) NOT IN (0,6) AND date_part(hr, nvl($2,sysdate)) >= 17 THEN 32400 + DATEDIFF(seconds,dateadd(hours,17,CAST(nvl($2,sysdate) AS date)),nvl($2,sysdate)) ELSE 0 END)
--substract first day if it is sunday and it is the same week
- (CASE WHEN DATE_PART(dow,$1) =0 and datediff ('week',$1,nvl ($2,sysdate)) =0 THEN DATEDIFF(seconds,$1,dateadd(days, 1, CAST($1 AS date)))
ELSE 0 END)
-- Substract time for Saturday if beginning on Sunday and ending on Saturday (the same week number)
- CASE WHEN DATE_PART(dow,nvl ($2,sysdate)) =6 and datediff ('week',$1,nvl ($2,sysdate)) =0 THEN datediff (seconds,TRUNC(nvl ($2,sysdate)),nvl ($2,sysdate)) ELSE 0 END

)/86400)*57600

END
AS DECIMAL(38,4))
$$ LANGUAGE sql
;
