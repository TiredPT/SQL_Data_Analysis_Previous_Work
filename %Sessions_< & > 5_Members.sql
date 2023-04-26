-- This query finds the % of sessions where the total # of members was either >= 5 OR < 5 that session.
-- It sums all percentages, per session, per week, being a dynamic query (past values don't change)

WITH AttendancePerSession as (

select 
        s.id as sessionId,
        s.groupId as groupId,
        CASE 
             WHEN ROW_NUMBER() OVER(PARTITION BY g.id ORDER BY s.date ASC) = 1 THEN "L1"
             WHEN ROW_NUMBER() OVER(PARTITION BY g.id ORDER BY s.date ASC) between 2 and 13 THEN "L2-L12"
             ELSE "L13+"
             END as tenure,

        CONCAT('https://pace.group/admin/group/',g.id) as groupLink,
        g.name as groupName,
        SUM(if(a.sessionJoinedAt >= s.date - INTERVAL 7 DAY, 1, 0)) as sessionAttendance, -- it only includes members
        COUNT(DISTINCT a.id) as possibleAttendance,
        IFNULL(ROUND(SUM(if(a.sessionJoinedAt >= s.date - INTERVAL 7 DAY, 1, 0))/COUNT(DISTINCT a.id)*100,1),0) as pctAttRatePerSession,
        DATE(s.date) as sessionDate,
        s.start as sessionStartTime,
        ROW_NUMBER() OVER(PARTITION BY g.id ORDER BY s.date ASC) AS nthGroupSession,
        DATE_SUB(DATE(s.date), INTERVAL DAYOFWEEK(s.date)-1 DAY) as weekCohort

    from paceprod.Sessions s
    join paceprod.Groups g on g.id = s.groupId 
    join paceprod.Attendances a on a.sessionId = s.id
    join GroupMembers gm on gm.id = a.memberId

    where 
        a.attended IS NOT NULL and -- added because of RSVP changes (auto session id creation) 
        s.start <= NOW() and 
        g.startDate IS NOT NULL and  
        g.isInternal is not true and 
        s.isPopUp = 0 and 
        g.isInternal is not true and 
        g.skipInMetrics != 1 and 
        g.groupTypeId != 'a54e3abf-69be-4115-bfe5-dd04fdc7d049' and 
        g.groupTypeId != 'd8ef8e02-e666-47b1-ba5b-55b2d02b66d5' and
        g.id != 'd67de29b-1dad-4a44-971b-2831ea49f47b' and
        g.id != 'b8e98060-85ad-46bc-b546-4369351db09b' and
        g.id != '6f711176-4ff1-4100-a3e3-14271a229b31' and 
        g.id != '8a184429-eb42-48b4-a3ea-1c7658524aa5' and 
        g.id != '22531d56-d5f7-4a27-8fae-cf79e07941b7' and 

        -- we just want members
        gm.isFacilitator IS NOT TRUE 

    group by   
        s.id 

    order by 
        s.date desc 

)

select     
    aps.weekCohort as "Week Cohort",
    ROUND(SUM(case when aps.sessionAttendance < 5 then 1 else 0 end)/COUNT(aps.sessionId)*100,2) as "% < 5 attendees",
    ROUND(SUM(case when aps.sessionAttendance >= 5 then 1 else 0 end)/COUNT(aps.sessionId)*100,2) as "% >= 5 attendees"

from AttendancePerSession aps 

group by
    aps.weekCohort 

order by 
    aps.weekCohort desc 

LIMIT 12
