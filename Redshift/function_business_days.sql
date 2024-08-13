CREATE FUNCTION f_weekday_minutes_between (timestamp without time zone, timestamp without time zone)
RETURNS numeric
STABLE
AS $$
SELECT 
CASE WHEN CAST(datediff(minutes,$1,nvl ($2,sysdate)) AS DECIMAL(38,4)) <= 0 
OR (DATE_PART(dow,$1) in (0,6) and DATE_PART(dow,nvl($2,sysdate)) in (0,6) and DATE_PART(dow,$1) = DATE_PART(dow,nvl($2,sysdate)) AND datediff ('week',$1,nvl ($2,sysdate)) =0)
    THEN CAST(0 as DECIMAL(38,4))  ELSE
CAST(datediff(minutes,$1,nvl($2,sysdate)) 
-- Subtract weekends
-CASE WHEN datediff ('week',$1,nvl ($2,sysdate)) !=0 THEN (1440*datediff ('week',$1,nvl ($2,sysdate))*2) ELSE 0 END
-- Add back time if beginning on a weekend
+ CASE WHEN DATE_PART(dow,$1) IN (0,6) and datediff ('week',$1,nvl ($2,sysdate)) !=0 THEN (datediff (minutes,TRUNC($1),$1)) ELSE 0 END
-- Add back time if ending on a weekend
+ CASE WHEN DATE_PART(dow,nvl ($2,sysdate)) IN (0,6) and datediff ('week',$1,nvl ($2,sysdate)) !=0 THEN (1440 - datediff (minutes,TRUNC(nvl ($2,sysdate)),nvl ($2,sysdate))) ELSE 0 END 
-- Substract time for Sunday if beginning on Sunday and ending on Saturday (the same week number)
- CASE WHEN DATE_PART(dow,$1) =0 and datediff ('week',$1,nvl ($2,sysdate)) =0 THEN (1440-datediff (minutes,TRUNC(nvl ($1,sysdate)),nvl ($1,sysdate))) ELSE 0 END
-- Substract time for Saturday if beginning on Sunday and ending on Saturday (the same week number)
- CASE WHEN DATE_PART(dow,nvl ($2,sysdate)) =6 and datediff ('week',$1,nvl ($2,sysdate)) =0 THEN datediff (minutes,TRUNC(nvl ($2,sysdate)),nvl ($2,sysdate)) ELSE 0 END
- CASE WHEN DATE_PART(dow,$1) =0 and DATE_PART(dow,nvl ($2,sysdate)) =6 and datediff ('week',$1,nvl ($2,sysdate)) >=1 THEN 2880 ELSE 0 END
- CASE WHEN ((DATE_PART(dow,$1) =6 and DATE_PART(dow,nvl ($2,sysdate)) =6) OR (DATE_PART(dow,$1) =0 and DATE_PART(dow,nvl ($2,sysdate))=0)
            OR (DATE_PART(dow,$1) =0 and DATE_PART(dow,nvl ($2,sysdate)) !=6) OR (DATE_PART(dow,nvl ($2,sysdate))=6 and DATE_PART(dow,$1) not in (0,6)))
     and datediff ('week',$1,nvl ($2,sysdate)) >=1 THEN 1440 ELSE 0 END
AS DECIMAL(38,4))
END
$$ LANGUAGE sql
;
