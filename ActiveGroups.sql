-- This query finds the total number of active groups in the last 7 days

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
