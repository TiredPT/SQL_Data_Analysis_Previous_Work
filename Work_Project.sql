-- THIS QUERY FINDS THE % OF SESSIONS WHERE THE TOTAL # OF MEMBERS WAS EITHER >= OR < 5 THAT SESSION.
-- IT SUMS ALL PERCENTAGES, PER SESSION, PER WEEK, BEING A DYNAMIC QUERY (PAST VALUES DON'T CHANGE).

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


-----------------------------------------------------------------------------------

-- FINDS ACTIVE GROUPS WITH FEWER THAN 7 MEMBERS -- 

with ActiveGroups as (
    select g.id as groupId,
           g.name as name,
           CONCAT(gt.nickname, '-', g.groupTypeCount) as groupNickname,
           g.startDate as start,
           g.endDate as end,
           g.isPopUp as isPopUp,
           count(distinct(gm.id)) as numMembers

    from paceprod.Groups g
        join paceprod.GroupTypes gt on g.groupTypeId = gt.id
        join paceprod.GroupMembers gm on g.id = gm.groupId and
            (
                gm.startDate is not null and
                gm.startDate < NOW()
            ) and
            (
                gm.endDate is null or
                gm.endDate > NOW()
            ) and
            gm.isFacilitator is not true
    where
        (
            g.startDate is not null and
            g.startDate < NOW()
        ) and
        (
            g.endDate is null or
            g.endDate > NOW()
        ) and
        g.isInternal is not true and
        g.skipInMetrics != 1 and
        g.isPopUp != 1 AND
        g.groupTypeId != 'a54e3abf-69be-4115-bfe5-dd04fdc7d049' AND
        g.groupTypeId != 'd8ef8e02-e666-47b1-ba5b-55b2d02b66d5' AND
        g.id != 'd67de29b-1dad-4a44-971b-2831ea49f47b' AND
        g.id != 'b8e98060-85ad-46bc-b546-4369351db09b' AND
        g.id != '6f711176-4ff1-4100-a3e3-14271a229b31'

    group by gm.groupId
    order by groupNickname

)

select ag.groupNickname,
       ag.numMembers,
       DATE(ag.start) as start,
       DATE(ag.end) as end,
       ag.groupId,
       ag.isPopUp
from ActiveGroups ag
where ag.numMembers < 7
group by ag.groupNickname
order by ag.groupNickname;


------------------------------------------------------------------------------------

-- THIS QUERY FINDS THE TOTAL # OF ACTIVE GROUPS IN THE LAST 7 DAYS

with ActiveGroups as (
    select g.id as groupId,
           g.name as name,
           CONCAT(gt.nickname, '-', g.groupTypeCount) as groupNickname,
           g.startDate as start,
           g.endDate as end,
           g.isPopUp as isPopUp,
           count(distinct(gm.id)) as members
    from paceprod.Groups g
        join paceprod.GroupTypes gt on g.groupTypeId = gt.id
        join paceprod.GroupMembers gm on g.id = gm.groupId and
            (
            gm.startDate is not null and
            gm.startDate < date_add(date_add('{{WEEK_START}}', interval 7 day), interval 7 hour)
            ) and
            (
                gm.endDate is null or
                gm.endDate > '{{WEEK_START}}'
            ) and
            gm.isFacilitator is not true
    where
        (
            g.startDate is not null and
            g.startDate < date_add(date_add('{{WEEK_START}}', interval 7 day), interval 7 hour)
        ) and
        (
            g.endDate is null or
            g.endDate > date_add('{{WEEK_START}}', interval 7 hour)
        ) and
        
        -- WE WANT ONLY CUSTOMER DATA
        g.isInternal is not true and
        g.skipInMetrics != 1 and
        g.groupTypeId != 'a54e3abf-69be-4115-bfe5-dd04fdc7d049' AND
        g.groupTypeId != 'd8ef8e02-e666-47b1-ba5b-55b2d02b66d5' AND
        g.id != 'd67de29b-1dad-4a44-971b-2831ea49f47b' AND
        g.id != 'b8e98060-85ad-46bc-b546-4369351db09b' AND
        g.id != '6f711176-4ff1-4100-a3e3-14271a229b31'
        AND g.isPopUp = 0

    group by gm.groupId
    order by groupNickname
),
attendance_summary AS (
    select
        g.id as groupId,
        DATE_FORMAT(CONVERT_TZ(max(a.sessionJoinedAt),'GMT','US/Pacific'),'%W') as session_day_of_week,
        DATE_FORMAT(CONVERT_TZ(min(a.sessionJoinedAt),'GMT','US/Pacific'),'%M %e') as first_session_date,
        DATE_FORMAT(CONVERT_TZ(max(a.sessionJoinedAt),'GMT','US/Pacific'),'%M %e') as recent_session_date,
        count(distinct a.sessionId) as num_sessions_with_attendance,
        SUM(if(a.sessionJoinedAt >= NOW() - INTERVAL 7 DAY, 1, 0)) as num_attended_in_past_week,
        SUM(if(a.sessionJoinedAt BETWEEN (NOW() - INTERVAL 14 DAY) AND (NOW() - INTERVAL 7 DAY), 1, 0)) as num_attended_in_prior_week,
        g.groupTypeCount,
        CONCAT(
        SUM(if(a.sessionJoinedAt BETWEEN (NOW() - INTERVAL 21 DAY) AND (NOW() - INTERVAL 14 DAY), 1, 0)), ' -- ',
        SUM(if(a.sessionJoinedAt BETWEEN (NOW() - INTERVAL 14 DAY) AND (NOW() - INTERVAL 7 DAY), 1, 0)), ' -- ',
        SUM(if(a.sessionJoinedAt >= NOW() - INTERVAL 7 DAY, 1, 0))
        ) as attendance_last_3wks
    from Attendances a
    join GroupMembers gm on gm.id = a.memberId
    join paceprod.Groups g on g.id = gm.groupId
    join Users u on u.id = gm.userId
    where u.isFacilitator = 0 AND
        a.attended = 1 AND
        u.adminFlags = 0 AND
        gm.isTempMember != 1
    group by g.id
    order by g.startDate desc
),
ActiveSessions as (
    select s.id as sessionId,
        s.isPopUp as isPopUp,
        s.groupId as groupId,
        ag.groupNickname as groupNickname
    from paceprod.Sessions s
        join ActiveGroups ag on ag.groupId = s.groupId
    where
        #DATE(s.date) <= date_add(date_add('{{WEEK_START}}', interval 7 day), interval 7 hour) AND
        DATE(s.start) >= '{{WEEK_START}}'
)
select ag.groupNickname,
    ag.members,
    ag.isPopUp,
    a.num_sessions_with_attendance as sessionsAttended,
    DATE(ag.start) as start,
    DATE(ag.end) as end,
    count(distinct acs.sessionId) as sessionCount,
    ag.groupId

from ActiveGroups ag
    left join ActiveSessions acs on acs.groupId=ag.groupId
    left join attendance_summary a on a.groupId=ag.groupId
group by ag.groupNickname
order by ag.groupNickname;
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



--------------------------------------------------------------------------

-- FINDS ATTENDANCE RATE PER SESSION (%)
-- %L1 (% attended first session)
-- %L2-L12 (% attended second -12th session)
-- %L13+ (% attended 13th and + session)

WITH AttendancePerSession as ( -- want to find att per nthGroupSession (easier to pull L1, etc)
    
    select 
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
),

TotalsPerWeek as (

select
    aps.weekCohort as weekCohort2,
    -- L1 totals
    SUM(case when aps.tenure = "L1" then 1 else 0 end) as totalL1,
    SUM(case when aps.tenure = "L1" then pctAttRatePerSession else 0 end) as pctL1,

    -- L2 - L12 totals
    SUM(case when aps.tenure = "L2-L12" then 1 else 0 end) as totalL2_L12,
    SUM(case when aps.tenure = "L2-L12" then pctAttRatePerSession else 0 end) as pctL2_L12,

    -- L13+ totals
    SUM(case when aps.tenure = "L13+" then 1 else 0 end) as total13_plus,
    SUM(case when aps.tenure = "L13+" then pctAttRatePerSession else 0 end) as pctL13_plus
from
    AttendancePerSession aps 

group by
    aps.weekCohort

order by 
    aps.weekCohort desc 
)

select
    tpw.weekCohort2 as "Week Cohort",
    IFNULL(ROUND(tpw.pctL1/tpw.totalL1,1),0) as "% by L1",
    IFNULL(ROUND(tpw.pctL2_L12/tpw.totalL2_L12,1),0) as "% by L2-L12",
    IFNULL(ROUND(tpw.pctL13_plus/tpw.total13_plus,1),0) as "% by L13+"

from 
    TotalsPerWeek tpw

group by 
    tpw.weekCohort2

order by 
    tpw.weekCohort2 desc 

LIMIT 12



-----------------------------------------------------------------------------------

-- FINGS THE GROUPS & RESPECTIVE SESSION TENURE
--	Groups @ L1 (first session)
--	Groups @ L2 - L12 (2nd and 12th session)
--	Groups @ L13+ (13th up sessions) 


WITH GroupSessions as (
    
    select 
        s.groupId as groupId,
        CASE 
             WHEN ROW_NUMBER() OVER(PARTITION BY g.id ORDER BY s.date ASC) = 1 THEN "L1"
             WHEN ROW_NUMBER() OVER(PARTITION BY g.id ORDER BY s.date ASC) between 2 and 13 THEN "L2-L12"
             ELSE "L13+"
             END as tenure,

        CONCAT('https://pace.group/admin/group/',g.id) as groupLink,
        g.name as groupName,
        DATE(s.date) as sessionDate,
        s.start as sessionStartTime,
        ROW_NUMBER() OVER(PARTITION BY g.id ORDER BY s.date ASC) AS nthGroupSession,
        DATE_SUB(DATE(s.date), INTERVAL DAYOFWEEK(s.date)-1 DAY) as weekCohort

    from paceprod.Sessions s
    join paceprod.Groups g on g.id = s.groupId 

    where
        s.start <= NOW() and 
        g.startDate IS NOT NULL and   
        s.isPopUp = 0 and 
        g.isInternal is not true and 
        g.skipInMetrics != 1 and 
        g.groupTypeId != 'a54e3abf-69be-4115-bfe5-dd04fdc7d049' and 
        g.groupTypeId != 'd8ef8e02-e666-47b1-ba5b-55b2d02b66d5' and
        g.id != 'd67de29b-1dad-4a44-971b-2831ea49f47b' and
        g.id != 'b8e98060-85ad-46bc-b546-4369351db09b' and
        g.id != '6f711176-4ff1-4100-a3e3-14271a229b31' and 
        g.id != '8a184429-eb42-48b4-a3ea-1c7658524aa5' and 
        g.id != '22531d56-d5f7-4a27-8fae-cf79e07941b7'

    group by   
        s.id 

    order by 
        s.date desc  
    
)

select
   gs.weekCohort as 'Week Cohort',
   SUM(case when gs.Tenure = "L1" then 1 else 0 end) as "L1",
   SUM(case when gs.Tenure = "L2-L12" then 1 else 0 end) as "L2-L12",
   SUM(case when gs.Tenure = "L13+" then 1 else 0 end) as "L13+",
   COUNT(gs.Tenure) as "Weekly Total"

from 
    GroupSessions gs 
group by
    gs.weekCohort
 
order by 
    gs.weekCohort desc 

LIMIT 12 


----------------------------------------------------------------------------------


-- FINDS THE # OF MEMBERS IN 1, 2, & 3 GROUPS PER WEEK

WITH ActiveMembers AS ( -- gives us members that are actively placed in groups

SELECT
    gm.userId as userId,
    gm.id as groupMemberId,
    g.id as groupId,
    #GROUP_CONCAT(g.name,' ') as groupName,
    CONCAT(u.firstName, ' ',u.lastName) as memberName,
    DATE(gm.startDate) as groupStartDate,
    DATE(gm.endDate) as groupEndDate, 
    COUNT(DISTINCT gm.id) as currentActiveGroups
    
    
FROM 
    paceprod.Users u 
join paceprod.GroupMembers gm on gm.userId = u.id 
join paceprod.Memberships m on m.userId = u.id 
join paceprod.Groups g on g.id = gm.groupId

where
    gm.startDate IS NOT NULL and -- were placed in a group at some point
    gm.endDate IS NULL and -- still active in the groups
    gm.isFacilitator IS NOT TRUE and -- only members 
    gm.isTempMember IS NOT TRUE and 

    -- removes all internal groups
    g.isInternal is not true and 
    g.skipInMetrics != 1 and 
    g.groupTypeId != 'a54e3abf-69be-4115-bfe5-dd04fdc7d049' and 
    g.groupTypeId != 'd8ef8e02-e666-47b1-ba5b-55b2d02b66d5' and
    g.groupTypeId != '59d5603c-f112-4687-8cb9-516467c91ae5' and 
    g.id != 'd67de29b-1dad-4a44-971b-2831ea49f47b' and
    g.id != 'b8e98060-85ad-46bc-b546-4369351db09b' and
    g.id != '6f711176-4ff1-4100-a3e3-14271a229b31' and 
    g.id != '8a184429-eb42-48b4-a3ea-1c7658524aa5' and 
    g.id != '22531d56-d5f7-4a27-8fae-cf79e07941b7'

group by 
    gm.userId    

),

ActiveSessions AS ( -- want the sessionWeek

    SELECT s.id AS sessionId,
           s.groupId AS groupId,
           s.date AS sessionDate,
           s.isPopUp,
           DATE_SUB(DATE(s.date), INTERVAL DAYOFWEEK(s.date)-1 DAY) as sessionWeek
      FROM paceprod.Sessions s
)

SELECT
    ass.sessionWeek as "Week Cohort",
    SUM(case when am.currentActiveGroups = 1 then 1 else 0 end) as 'Members in 1 Group',
    SUM(case when am.currentActiveGroups = 2 then 1 else 0 end) as 'Members in 2 Groups',
    SUM(case when am.currentActiveGroups = 3 then 1 else 0 end) as 'Members in 3 Groups',
    SUM((case when am.currentActiveGroups = 1 then 1 else 0 end) +
        (case when am.currentActiveGroups = 2 then 1 else 0 end) +
        (case when am.currentActiveGroups = 3 then 1 else 0 end)) as 'Total'

FROM
    ActiveMembers am 

join ActiveSessions ass on ass.groupId = am.groupId

where
    ass.sessionWeek <= NOW()

group by    
    ass.sessionWeek 

order by   
    ass.sessionWeek desc 

LIMIT 12


------------------------------------------------------------------------------

-- FINDS ALL MEMBERS PLACED IN NEW GROUPS THE LAST 12 WEEKS


WITH PlacedMembers AS (

select 
    g.id as groupId,
    gm.id as group_member_id,
    g.name as groupName,
    CONCAT(u.firstName, " ",u.lastName) as memberName,
    g.startDate as startDate,
    gm.startDate as memberStartDate,
    #COUNT(DISTINCT gm.id) as totalPlaced,
    DATE_SUB(DATE(gm.startDate), INTERVAL DAYOFWEEK(gm.startDate)-1 DAY) as weekCohort

from paceprod.Groups g 
join paceprod.GroupMembers gm on gm.groupId = g.id
left join paceprod.Users u on u.id = gm.userId
where
    -- we don't want internal groups
    g.isInternal is not true and 
    g.skipInMetrics != 1 and 
    g.groupTypeId != 'a54e3abf-69be-4115-bfe5-dd04fdc7d049' and 
    g.groupTypeId != 'd8ef8e02-e666-47b1-ba5b-55b2d02b66d5' and
    g.groupTypeId != '59d5603c-f112-4687-8cb9-516467c91ae5' and 
    g.id != 'd67de29b-1dad-4a44-971b-2831ea49f47b' and
    g.id != 'b8e98060-85ad-46bc-b546-4369351db09b' and
    g.id != '6f711176-4ff1-4100-a3e3-14271a229b31' and 
    g.id != '8a184429-eb42-48b4-a3ea-1c7658524aa5' and 
    g.id != '22531d56-d5f7-4a27-8fae-cf79e07941b7' and 

    -- we don't want facilitators
    gm.isFacilitator IS NOT TRUE  


-- we want those placed prior to group start (new groups)
having gm.startDate <= g.startDate+INTERVAL 1 HOUR -- interval to make up for UTC/PST time differences if member was added the day of session (or readded)

order by
    g.startDate desc 
)

select 
    pm.weekCohort as "Week",
    COUNT(DISTINCT pm.group_member_id) as "Total Placed in NG"

from PlacedMembers pm 

group by 
    pm.weekCohort 

order by 
    pm.weekCohort desc 

LIMIT 12

--------------------------------------------------------------------------

-- FINDS HOW MANY GROUPS HAVE ENDED PER WEEK

WITH SunsettingGroups AS (

select    
    g.id as groupId,
    g.name as groupName,
    DATE(g.startDate) as startDate,
    DATE(g.endDate) as endDate,
    DATE_SUB(DATE(g.endDate), INTERVAL DAYOFWEEK(g.endDate)-1 DAY) as endDateWeek
from 
    paceprod.Groups g 
where 
    g.startDate IS NOT NULL and 
    g.endDate IS NOT NULL

    # We need to get Groups to exclude internal and other random groups.
        AND g.isInternal is not true 
        AND g.skipInMetrics != 1 
        AND g.groupTypeId != 'a54e3abf-69be-4115-bfe5-dd04fdc7d049'
        AND g.groupTypeId != 'd8ef8e02-e666-47b1-ba5b-55b2d02b66d5'
        AND g.groupTypeId != '59d5603c-f112-4687-8cb9-516467c91ae5'
        AND g.id != 'd67de29b-1dad-4a44-971b-2831ea49f47b'
        AND g.id != 'b8e98060-85ad-46bc-b546-4369351db09b'
        AND g.id != '6f711176-4ff1-4100-a3e3-14271a229b31' 

order by
    g.startDate desc
)

select

    sg.endDateWeek as 'Week Cohort',
    COUNT(sg.groupName) as "Sunsetting by EOW"

from 
    SunsettingGroups sg 

where
    sg.endDateWeek <= NOW()

group by 
    sg.endDateWeek 

order by 
    sg.endDateWeek desc  

LIMIT 12

