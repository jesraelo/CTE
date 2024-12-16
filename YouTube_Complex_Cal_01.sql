-- John Esraelo  20241216
-- GitHub link   https://github.com/jesraelo/CTE
-- File Name     YouTube_Complex_Cal_01.sql

/* ===========================================================================================
In this session: 
	> Drop <table> if exists <#tableName>.
	> set nocount on   -- reduce noise and traffic.
	> Variable declarations.
	> We have covered various types of temp tables, so in here.
	> Various Date and Time functions and manipulations.
	> CTE, standard, recursive w/UNION ALL (must have ALL for recursive CTE), and the calls to.
	> In spite of many developers' opinion; there is still a need for cursors
		> 3 levels of cursors:
			> Week Number
			> Week Day
			> Day to process
	> Optionally, create a stored procedure.

Segments:
	> declarations, and input parameters
	> Part 1
		> establish the number of days to process (this example: 1 month)
		> display calendar dimension (useful in cubes and data warehouse)
	> Part 2
		> display the month in paper calendar fashion, or like in UNIX
===========================================================================================  */

drop table if exists #SourceCalendar ; 
drop table if exists #OutPut ; 

set nocount on ; 

-- Initial declaration: Query Parameters as input to establish the base calendar and range
Declare 
    @TargetYear smallint = 2024 ,
	@TodayMonth smallint = 12 , 
	@TodayDay   smallint = DATEPART(day, getdate()) ;
	                                                            -- right() concats 0 if needed
DECLARE @StartDate  date = cast(@TargetYear as char(4)) + right('00'+cast(@TodayMonth as varchar(2)),2) + '01';
DECLARE @CutoffDate date = DATEADD(DAY, -1, DATEADD(MONTH, 1, @StartDate));
                                                         --Number of months to display  
;WITH NumberOfDays(n) AS 
(
  SELECT 0 
  UNION ALL                                               -- in this example, we have 31 days
  SELECT n + 1 	FROM NumberOfDays WHERE n < DATEDIFF(DAY, @StartDate, @CutoffDate)  -- number of days to process

), DayToProcess(d) 
AS 
(                     -- NOTE: NumberOfDays starts w/0, so we won't skip the first day
  SELECT DATEADD(DAY, n, @StartDate) FROM NumberOfDays

), Source 
AS
(
  SELECT
    TheDate          = CONVERT(date,       d),
    TheDay           = DATEPART(DAY,       d),
    TheDayName       = DATENAME(WEEKDAY,   d),
    TheDayName2Chars = left(DATENAME(WEEKDAY,   d),2),
    TheWeek          = DATEPART(WEEK,      d),
    TheISOWeek       = DATEPART(ISO_WEEK,  d),
    TheDayOfWeek     = DATEPART(WEEKDAY,   d),
    TheMonth         = DATEPART(MONTH,     d),
    TheMonthName     = DATENAME(MONTH,     d),
    TheQuarter       = DATEPART(Quarter,   d),
    TheYear          = DATEPART(YEAR,      d),
    TheFirstOfMonth  = DATEFROMPARTS(YEAR (d), MONTH(d), 1),
    TheLastOfYear    = DATEFROMPARTS(YEAR (d), 12, 31),
    TheDayOfYear     = DATEPART(DAYOFYEAR, d)
  FROM DayToProcess
)
SELECT * 
into #SourceCalendar 
FROM Source
  ORDER BY TheDate
    OPTION (MAXRECURSION 0);
--======================================================================================================
	select * from #SourceCalendar
--======================================================================================================

-- create the final output temporary table 
create table #OutPut 
(
	Weeknumber smallint, 
	TheDayOfWeek smallint, 
	Su char(2) default(''),
	Mo char(2) default(''),
	Tu char(2) default(''),
	We char(2) default(''),
	Th char(2) default(''),
	Fr char(2) default(''),
	Sa char(2) default('')
)

declare @WeekNumber smallint , @WeekDay smallint , @TheDay smallint, @Line varchar(max)='', @TheDayName2Chars char(2)='';

declare WeekCursor cursor for select distinct TheWeek from #SourceCalendar ; 
open WeekCursor ;
fetch next from WeekCursor into @WeekNumber ; 

while @@FETCH_STATUS = 0 
begin
	insert #OutPut (Weeknumber) values(@WeekNumber) ; 
	declare DayOfWeekCursor cursor for select TheDayOfWeek from #SourceCalendar where TheWeek = @WeekNumber;

	open DayOfWeekCursor ; 
	Fetch next from DayOfWeekCursor into @WeekDay ;
	while @@FETCH_STATUS = 0 
	begin
		update #OutPut set TheDayOfWeek = @WeekDay where Weeknumber = @WeekNumber ;

		declare TheDay cursor for 
			select TheDay, TheDayName2Chars 
			from #SourceCalendar 
			where TheDayOfWeek = @WeekDay and TheWeek = @WeekNumber; 

		open TheDay ;
		Fetch next from TheDay into @TheDay, @TheDayName2Chars ;
		while @@FETCH_STATUS = 0
		BEGIN
			update #OutPut set Su = right('00'+cast(@TheDay as varchar(2)),2) where @TheDayName2Chars  = 'Su' and TheDayOfWeek = @WeekDay and WeekNumber = @WeekNumber; 
			update #OutPut set Mo = right('00'+cast(@TheDay as varchar(2)),2) where @TheDayName2Chars  = 'Mo' and TheDayOfWeek = @WeekDay and WeekNumber = @WeekNumber; 
			update #OutPut set Tu = right('00'+cast(@TheDay as varchar(2)),2) where @TheDayName2Chars  = 'Tu' and TheDayOfWeek = @WeekDay and WeekNumber = @WeekNumber; 
			update #OutPut set We = right('00'+cast(@TheDay as varchar(2)),2) where @TheDayName2Chars  = 'We' and TheDayOfWeek = @WeekDay and WeekNumber = @WeekNumber; 
			update #OutPut set Th = right('00'+cast(@TheDay as varchar(2)),2) where @TheDayName2Chars  = 'Th' and TheDayOfWeek = @WeekDay and WeekNumber = @WeekNumber; 
			update #OutPut set Fr = right('00'+cast(@TheDay as varchar(2)),2) where @TheDayName2Chars  = 'Fr' and TheDayOfWeek = @WeekDay and WeekNumber = @WeekNumber; 
			update #OutPut set Sa = right('00'+cast(@TheDay as varchar(2)),2) where @TheDayName2Chars  = 'Sa' and TheDayOfWeek = @WeekDay and WeekNumber = @WeekNumber; 
			Fetch next from TheDay into @TheDay, @TheDayName2Chars  ;
		END 
		close TheDay ; 
		deallocate TheDay ; 
		Fetch next from DayOfWeekCursor into @WeekDay ;
	end 
	close DayOfWeekCursor
	deallocate DayOfWeekCursor 
	fetch next from WeekCursor into @WeekNumber ; 
end 
close WeekCursor ; 
deallocate WeekCursor ; 

/*
select * from #SourceCalendar ;
select * from #output ; 
*/ 

select 
	   @TargetYear [Year],   
	   @TodayMonth [Month],
	   @TodayDay   [Today],
	   replicate('--',3) [<    >], 
	   Su, Mo, Tu, We, Th, Fr, Sa 
from #output

