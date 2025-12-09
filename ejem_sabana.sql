
GO
CREATE OR ALTER PROCEDURE Work.WRK_ODS_UPD_Fact_SalesOrder_PARDirectorCampaign
	@YearCampaign INT = NULL,
	@DiferencialFlag INT = 1
AS
BEGIN

SET NOCOUNT ON
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

DECLARE	@StoreProcedureName VARCHAR(800) = CONCAT('Work.WRK_ODS_UPD_Fact_SalesOrder_PARDirectorCampaign ', @YearCampaign, ', ', @DiferencialFlag)

INSERT INTO Work.LogExecuteHst WITH(TABLOCK)
SELECT	datechange,
		ObjectName,
		Comment
FROM	Work.LogExecute WITH(NOLOCK)
WHERE	ObjectName = @StoreProcedureName

DELETE	Work.LogExecute WITH(TABLOCK)
WHERE	ObjectName = @StoreProcedureName

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 01: INICIO')

DECLARE	@Today DATETIME = GETDATE(),
		@Year INT,
		@Campaign INT,
		@YearCampaignToday INT

SELECT	@Year = LEFT(@YearCampaign, 4),
		@Campaign = CONVERT(INT, RIGHT(@YearCampaign, 2))

SELECT	@YearCampaignToday = Campaign
FROM	dbo.V_PeriodCampaign P WITH(NOLOCK)
WHERE	@Today >= StartDate
AND		@Today < EndDate

SELECT	T1.CountryCode,
		T1.Code PlanCode,
		T1.Calendar,
		T1.Id PlanId
INTO	#tmpCalendarPAR
FROM	[Temporary].[tmp_ODS_Core_Plan_Local] T1 WITH(NOLOCK)
WHERE	UPPER(T1.Code) LIKE 'PAR%'
AND		(LEN(T1.Code) = 8 OR LEN(T1.Code) = 5)
AND		T1.Active = 1

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 02: INSERT INTO #tmpCalendarPAR')

SELECT	P.CountryCode,
		Id,
		RIGHT(Code, 4) + '' + SUBSTRING(Code, 2, 2) Campaign,
		StartDate,
		EndDate
INTO	#tmpPeriodCampaign_new
FROM	dbo.V_Period P WITH(NOLOCK)
INNER JOIN #tmpCalendarPAR C
ON		P.Calendar = C.PlanCode
AND		P.CountryCode = C.CountryCode

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 03: SELECT INTO #tmpPeriodCampaign_new')

DECLARE @tmpPeriodCampaign TABLE(
		CountryCode VARCHAR(5),
		Campaign INT,
		Id INT)

IF @YearCampaign IS NULL
	INSERT INTO @tmpPeriodCampaign
	SELECT	CountryCode,
			Campaign,
			Id
	FROM	#tmpPeriodCampaign_new WITH(NOLOCK)
	WHERE	@Today >= StartDate
	AND		@Today < EndDate
ELSE
	INSERT INTO @tmpPeriodCampaign
	SELECT	CountryCode,
			Campaign,
			Id
	FROM	#tmpPeriodCampaign_new WITH(NOLOCK)
	WHERE	Campaign = @YearCampaign

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 04: INSERT INTO @tmpPeriodCampaign')

SELECT	CountryCode,
		Calendar,
		Code
INTO	#tmpDelIndexPAR
FROM	Temporary.tmp_ODS_Core_Plan_Local WITH(NOLOCK)
WHERE	Code = 'DelIndexPAR'
AND		Active = 1

SELECT	CountryCode,
		Calendar,
		Code
INTO	#tmpDelIndex
FROM	Temporary.tmp_ODS_Core_Plan_Local WITH(NOLOCK)
WHERE	Code = 'DelIndex'
AND		Active = 1

SELECT	CountryCode,
		PlanId,
		Calendar,
		PlanCode
INTO	#PlanPAR
FROM	#tmpCalendarPAR
WHERE	PlanCode LIKE CONCAT('%PAR', RIGHT(@Year, 2), '%')

SELECT	C.CountryCode,
		P.PlanId,
		P.Calendar CalendarPAR,
		P.PlanCode,
		CONVERT(DATETIME, MAX(IIF(C.Code = 'PAR_START_DATE', REPLACE(REPLACE(C.Expression, 'Cast (''', ''), ''' AS datetime)', ''), NULL))) StartDatePAR, 
		CONVERT(DATETIME, MAX(IIF(C.Code = 'PAR_END_DATE', REPLACE(REPLACE(C.Expression, 'Cast (''', ''), ''' AS datetime)', ''), NULL))) EndDatePAR,
		CONVERT(INT, 0) PeriodCOM,
		CONVERT(INT, 0) NroCampaignPARPending,
		CONVERT(VARCHAR(100), NULL) CalendarCOM,
		CONVERT(DATETIME, NULL) StartDateCOM,
		CONVERT(DATETIME, NULL) EndDateCOM,
		CONVERT(INT, 0) PeriodAfterCOM,
		CONVERT(VARCHAR(10), 0) CampaingAfterCOM,
		CONVERT(INT, 0) YearCampaignAfter
INTO	#PeriodPAR
FROM	Temporary.tmp_ODS_Core_PlanParameter_Local C WITH (NOLOCK)
INNER JOIN #PlanPAR P
ON		C.CountryCode = P.CountryCode
AND		C.[Plan] = P.PlanId
WHERE	C.Code IN ('PAR_START_DATE', 'PAR_END_DATE')
GROUP BY C.CountryCode,
		P.PlanId,
		P.Calendar,
		P.PlanCode

CREATE CLUSTERED INDEX IDX_#PeriodPAR ON #PeriodPAR(CountryCode, PlanId)

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 05: SELECT INTO #PeriodPAR')

SELECT	PC.CountryCode,
		PC.Id PeriodCOM,
		'Campaign' CalendarCOM,
		PC.StartDate StartDateCOM,
		PC.EndDate EndDateCOM,
		PC.PlanId,
		PC.PlanCode,
		CONVERT(INT, 0) PeriodAfterCOM,
		CONVERT(VARCHAR(10), 0) CampaingAfterCOM,
		CONVERT(INT, 0) YearCampaignAfter,
		13 - PC.RowNumber NroCampaignPARPending
INTO	#PendingPAR
FROM	(SELECT	PA.CountryCode,
				Id, 
				Campaign,
				ROW_NUMBER() OVER(PARTITION BY (PA.CountryCode)ORDER BY StartDate) RowNumber,
				P.PlanId,
				PA.StartDate,
				PA.EndDate,
				P.PlanCode
		FROM	V_PeriodCampaign PA WITH(NOLOCK)
		INNER JOIN #PeriodPAR P
		ON		PA.EndDate > P.StartDatePAR
		AND		PA.StartDate < P.EndDatePAR
		AND		PA.CountryCode = P.CountryCode) PC
WHERE	CONVERT(SMALLINT, RIGHT(PC.Campaign, 2)) = @Campaign

CREATE CLUSTERED INDEX IDX_#PendingPAR ON #PendingPAR(CountryCode, CalendarCOM, PeriodAfterCOM)

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 06: SELECT INTO #PendingPAR')

UPDATE	#PendingPAR
SET		PeriodAfterCOM = dbo.toPeriodsAfter(CountryCode, 'Campaign', PeriodCOM, 1)

UPDATE	PP
SET		CampaingAfterCOM = CONCAT('C',CONVERT(VARCHAR(5), RIGHT(LEFT(P.Code, 3), 2))),
		YearCampaignAfter = CONVERT(INT, CONCAT(RIGHT(P.Code, 4),RIGHT(LEFT(P.Code, 3), 2)))
FROM	#PendingPAR PP
INNER JOIN Temporary.tmp_ODS_Core_Period_Local P WITH(NOLOCK)
ON		PP.CountryCode = P.CountryCode
AND		PP.CalendarCOM = P.CalendarCode
AND		PP.PeriodAfterCOM = P.Id
AND		P.Code <> 'Origin'

UPDATE	P
SET		PeriodCOM = PE.PeriodCOM,
		NroCampaignPARPending = PE.NroCampaignPARPending,
		CalendarCOM = PE.CalendarCOM,
		StartDateCOM = PE.StartDateCOM,
		EndDateCOM = PE.EndDateCOM,
		PeriodAfterCOM = PE.PeriodAfterCOM,
		CampaingAfterCOM = PE.CampaingAfterCOM,
		YearCampaignAfter = PE.YearCampaignAfter
FROM	#PeriodPAR P
INNER JOIN #PendingPAR PE
ON		P.CountryCode = PE.CountryCode
AND		P.PlanId = PE.PlanId

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 07: UPDATE #PendingPAR')

SELECT	C.CountryCode,
		C.Id PeriodPAR,
		C.StartDate StartCampaignPAR,
		C.EndDate EndCampaignPAR,
		P.NroCampaignPARPending,
		P.PeriodCOM,
		P.CalendarCOM,
		P.StartDateCOM,
		P.EndDateCOM,
		P.PeriodAfterCOM,
		P.CampaingAfterCOM,
		P.YearCampaignAfter,
		P.PlanCode,
		CONVERT(INT, 0 ) PeriodBeforePAR3
INTO	#CorePeriod
FROM	Temporary.tmp_ODS_Core_Period_Local C WITH(NOLOCK)
INNER JOIN #PeriodPAR P
ON		C.CountryCode = P.CountryCode
AND		C.Calendar = P.CalendarPAR
AND		C.EndDate > P.StartDatePAR
AND		C.StartDate < P.EndDatePAR
AND		SUBSTRING(Code, 2, 2) = RIGHT(CONCAT('00', @Campaign), 2)

UPDATE	#CorePeriod
SET		PeriodBeforePAR3 = dbo.toPeriodsBefore(CountryCode, PlanCode, PeriodPAR, 2)

CREATE TABLE #PeriodIndexPar (Id INT, CountryCode VARCHAR(3))
CREATE TABLE #PeriodIndex(Id INT, CountryCode VARCHAR(3), PeriodCOMLast1 INT, CalendarCOM VARCHAR(100), PeriodCOMLast3 INT)

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 08: SELECT INTO #CorePeriod')

IF @YearCampaign < @YearCampaignToday
BEGIN
	INSERT INTO #PeriodIndexPar
	SELECT	c.Id,
			c.CountryCode
	FROM	Temporary.tmp_ODS_Core_Period_Local c WITH(NOLOCK)
	INNER JOIN #tmpDelIndexPAR p
	ON		C.Calendar = p.Calendar
	AND		C.CountryCode = p.CountryCode
	INNER JOIN #CorePeriod CO
	ON		CO.CountryCode = C.CountryCode
	AND		CO.EndCampaignPAR BETWEEN C.StartDate AND C.EndDate

	INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 08.1: INSERT INTO #PeriodIndexPar')

	INSERT INTO #PeriodIndex
	SELECT	C.Id,
			C.CountryCode,			
			Co.PeriodCOM,
			co.CalendarCOM,
			0 PeriodCOMLast3
	FROM	Temporary.tmp_ODS_Core_Period_Local C WITH(NOLOCK)
	INNER JOIN #tmpDelIndex P
	on		C.Calendar = p.Calendar
	AND		C.CountryCode = p.CountryCode
	INNER JOIN #CorePeriod CO
	ON		CO.CountryCode = C.CountryCode
	AND		CO.EndDateCOM BETWEEN C.StartDate AND C.EndDate
	WHERE	C.Code <> 'Origin'
	AND		RIGHT(LEFT(C.Code, 5), 1) = 4

	UPDATE	#PeriodIndex
	SET		PeriodCOMLast3 = dbo.toPeriodsBefore(CountryCode, CalendarCOM, PeriodCOMLast1, 2)

	INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 08.2: INSERT INTO #PeriodIndex')
END
ELSE
BEGIN
	SELECT	c.Id,
			c.CountryCode,
			ROW_NUMBER() OVER(PARTITION BY C.CountryCode ORDER BY Id DESC) N
	INTO	#PeriodIndexPar_old
	FROM	Temporary.tmp_ODS_Core_Period_Local c WITH(NOLOCK)
	INNER JOIN #tmpDelIndexPAR p
	ON		c.Calendar = p.Calendar
	AND		C.CountryCode = p.CountryCode
	INNER JOIN #CorePeriod CO
	ON		CO.CountryCode = C.CountryCode
	AND		C.StartDate >= co.StartCampaignPAR
	AND		C.EndDate < GETDATE()
	ORDER BY Id DESC
	
	DELETE FROM #PeriodIndexPar_old
	WHERE	N > 1

	INSERT INTO #PeriodIndexPar
	SELECT	Id,
			CountryCode
	FROM	#PeriodIndexPar_old

	INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 08.1: INSERT INTO #PeriodIndexPar')

	SELECT	c.Id,
			c.CountryCode,
			ROW_NUMBER() OVER(PARTITION BY C.CountryCode ORDER BY Id DESC) N,
			CO.CalendarCOM,
			Co.PeriodCOM,
			CONVERT(INT, 0) PeriodCOMLast1,
			CONVERT(INT, 0) PeriodCOMLast3
	INTO	#PeriodIndex_old
	FROM	Temporary.tmp_ODS_Core_Period_Local C WITH(NOLOCK)
	INNER JOIN #tmpDelIndex p
	ON		c.Calendar = p.Calendar
	AND		C.CountryCode = p.CountryCode
	INNER JOIN #CorePeriod CO
	ON		CO.CountryCode = C.CountryCode
	AND		C.StartDate >= co.StartDateCOM
	AND		C.EndDate < GETDATE()
	ORDER BY Id DESC

	DELETE FROM #PeriodIndex_old
	WHERE	N > 1
	
	UPDATE	#PeriodIndex_old
	SET		PeriodCOMLast1 = dbo.toPeriodsBefore(CountryCode, CalendarCOM, PeriodCOM, 1)

	UPDATE	#PeriodIndex_old
	SET		PeriodCOMLast3 = dbo.toPeriodsBefore(CountryCode, CalendarCOM, PeriodCOMLast1, 2)

	INSERT INTO #PeriodIndex
	SELECT	Id,
			CountryCode,
			PeriodCOMLast1,
			CalendarCOM,
			PeriodCOMLast3
	FROM	#PeriodIndex_old

	INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 08.2: INSERT INTO #PeriodIndex')
END

SELECT	@YearCampaign YearCampaign,
		Q.CountryCode,
		CONVERT(VARCHAR(100), NULL) NextCampaign,
		Q.ActivityCenter,
		Q.Code,
		Q.DirDemoteDate,
		Q.DirDemoteDt,
		Q.Name Name1,
		CONVERT(VARCHAR(1000), NULL) [Name],
		CONVERT(VARCHAR(100), NULL) InicioPAR,
		Q.PARTitle,
		Q.NewDDirCnt,
		Q.CGGV_3MthAvg,
		Q.NewDirector,
		Q.TotalFamilySales VtaTotalAcumFamActualaCX,
		Q.TotalGVSales VtaTotalAcumGrpPerPARActualCX,
		Q.CGGV VtaFamRealenCX,
		Q.GVDGV VntGrpPerPARRealenCX,
		CONVERT(MONEY, 0.00) PB_GF,
		CONVERT(MONEY, 0.00) VtaFamObjEvent_Conv,
		CONVERT(MONEY, 0.00) VtaFamObjCamp_Conv,
		CONVERT(MONEY, 0.00) PB_GP_PAR,
		CONVERT(MONEY, 0.00) VtaGPPARObjEvent_Conv,
		CONVERT(MONEY, 0.00) VtaGPPARObjCamp_Conv,
		CONVERT(MONEY, 0.00) VtaFamObjEvent_Jun,
		CONVERT(MONEY, 0.00) VtaFamObjCamp_Jun,
		CONVERT(MONEY, 0.00) VtaGPPARObjEvent_Jun,
		CONVERT(MONEY, 0.00) VtaGPPARObjCamp_Jun,
		CONVERT(MONEY, 0.00) VtaFamObjEvent_Int,
		CONVERT(MONEY, 0.00) VtaFamObjCamp_Int,
		CONVERT(MONEY, 0.00) VtaFamObjEvent_Gal,
		CONVERT(MONEY, 0.00) VtaFamObjCamp_Gal,
		CONVERT(MONEY, 0.00) VtaFamObjEvent_Gal_Aco,
		CONVERT(MONEY, 0.00) VtaFamObjCamp_Gal_Aco,
		CONVERT(MONEY, 0.00) AVG_CGGV,
		CONVERT(MONEY, 0.00) AVG_GVDGV,
		CONVERT(MONEY, 0.00) Num_VtaFam,
		CONVERT(MONEY, 0.00) Num_VtaGPPAR,
		CONVERT(MONEY, 0.00) DirPodriaGanar_Fam_Con,
		CONVERT(MONEY, 0.00) DirPodriaGanar_GPPAR_Con,
		CONVERT(MONEY, 0.00) DirPodriaGanar_Fam_Jun,
		CONVERT(MONEY, 0.00) DirPodriaGanar_GPPAR_Jun,
		CONVERT(MONEY, 0.00) DirPodriaGanar_Fam_Int,
		CONVERT(MONEY, 0.00) DirPodriaGanar_Fam_Gal,
		CONVERT(MONEY, 0.00) DirPodriaGanar_Fam_Gal_Aco,
		CONVERT(VARCHAR(100), NULL) DirPodriaGanar,
		Q.FOPARPeriodAccum,
		CONVERT(INT, 0) PPEDFaltan,
		Q.GVPARPeriodCnt,
		CONVERT(MONEY, 0.00) DelinqencyIdxPGPAR_Rec,
		CONVERT(MONEY, 0.00) DelinqencyIdxFamily_Rec,
		CONVERT(MONEY, 0.00) DelinqencyIdxPGPAR,
		CONVERT(MONEY, 0.00) DelinqencyIdxFamily,
		G.Left_Placement,
		G.Right_Placement,
		G.CordinatorActivityCenter,
		G.CoordinatorCode,
		G.Level_Placement,
		G.CoordinatorLevel_SalesCoordinator,
		CONVERT(DATETIME, NULL) TitleDate,
		CONVERT(INT, 0) FlagNew,
		CONVERT(MONEY, 0.00) MRM,
		CONVERT(MONEY, 0.00) NextMRM,
		CONVERT(INT, 0) YearCampaignAfter
INTO	#PAR1
FROM	Genealogy.GenealogyDirector G WITH(NOLOCK)
INNER JOIN Temporary.tmp_ODS_Earnings_PlanPARQual_Local Q WITH(NOLOCK)
ON		G.ActivityCenter = Q.ActivityCenter 
AND		G.CountryCode = Q.CountryCode
AND		(Q.CopyFlag = 0 OR @DiferencialFlag = 0)
INNER JOIN @tmpPeriodCampaign P
ON		Q. CountryCode = P.CountryCode
AND		Q.Period = P.Id
AND		G.YearCampaign = P.Campaign
AND		(G.IsReadedPAR = 0 OR @DiferencialFlag = 0)

CREATE CLUSTERED INDEX IDX_#PAR1 ON #PAR1(CountryCode, ActivityCenter)
CREATE NONCLUSTERED INDEX IDX_#PAR1_1 ON #PAR1(CountryCode)
CREATE NONCLUSTERED INDEX IDX_#PAR1_2 ON #PAR1(CountryCode, Code)
CREATE NONCLUSTERED INDEX IDX_#PAR1_3 ON #PAR1(CountryCode, PARTitle)
CREATE NONCLUSTERED INDEX IDX_#PAR1_4 ON #PAR1(CountryCode, YearCampaign)

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 09: SELECT INTO #PAR1')

UPDATE	#PAR1
SET		[Name] = CASE WHEN DirDemoteDt IS NOT NULL THEN CAST(Name1 + ' *' AS VARCHAR) ELSE Name1 END

UPDATE	P
SET		InicioPAR = TC.Code
FROM	#PAR1 P
INNER JOIN V_TitleCode TC WITH(NOLOCK)
ON		P.CountryCode = TC.CountryCode
AND		P.PARTitle = TC.[Rank]

UPDATE	P
SET		NextCampaign = C.CampaingAfterCOM,
		YearCampaignAfter = C.YearCampaignAfter
FROM	#PAR1 P
INNER JOIN #CorePeriod C
ON		P.CountryCode = C.CountryCode

UPDATE	P
SET		TitleDate = A.TitleDate
FROM	#PAR1 p
INNER JOIN Temporary.tmp_ODS_Genealogy_Title_Local A WITH(NOLOCK)
ON		P.CountryCode = A.CountryCode
AND		P.ActivityCenter = A.ActivityCenter
AND		A.[Rank] = 70

UPDATE	#PAR1
SET		FlagNew = 1
FROM	#PAR1 P
INNER JOIN #PeriodPAR C
ON		P.CountryCode = C.CountryCode
WHERE	P.TitleDate >= C.StartDatePAR
AND		P.TitleDate < C.EndDatePAR

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 10: UPDATE #PAR1')

SELECT	P.CountryCode,
		P.ActivityCenter,
		AVG(Q.CGGV) PromCGGV,
		AVG(Q.GVDGV) PromGVDGV
INTO	#Pro3UltCamp
FROM	#PAR1 P
INNER JOIN Temporary.tmp_ODS_Earnings_PlanPARQual_Local Q WITH(NOLOCK)
ON		P.CountryCode = Q.CountryCode
AND		P.ActivityCenter = Q.ActivityCenter
INNER JOIN #CorePeriod C
ON		Q.CountryCode = C.CountryCode
AND		Q.Period BETWEEN PeriodBeforePAR3 AND PeriodPAR
GROUP BY P.ActivityCenter,
		P.CountryCode

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 11: SELECT INTO #Pro3UltCamp')

UPDATE	P
SET		AVG_CGGV = PU.PromCGGV,
		AVG_GVDGV = PU.PromGVDGV
FROM	#PAR1 P
INNER JOIN #Pro3UltCamp PU
ON		P.CountryCode = PU.CountryCode
AND		P.ActivityCenter = PU.ActivityCenter

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 12: UPDATE #PAR1')

UPDATE	P
SET		Num_VtaFam = (VtaTotalAcumFamActualaCX + AVG_CGGV) * C.NroCampaignPARPending
FROM	#PAR1 P
INNER JOIN #CorePeriod C
ON		P.CountryCode = C.CountryCode

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 13: UPDATE #PAR1')

UPDATE	P
SET		Num_VtaGPPAR = (VtaTotalAcumGrpPerPARActualCX + AVG_GVDGV) * C.NroCampaignPARPending
FROM	#PAR1 P
INNER JOIN #CorePeriod C
ON		P.CountryCode = C.CountryCode

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 14: UPDATE #PAR1')

UPDATE	P
SET		PB_GF = PB.PB_GF,
		PB_GP_PAR = PB.PB_GP_PAR
FROM	#PAR1 P
INNER JOIN Temporary.tmp_ODS_YanbalITSupport_PBPAR_Local PB WITH(NOLOCK)
ON		P.CountryCode = PB.CountryCode
AND		P.Code = PB.Code

UPDATE	P
SET		VtaFamObjEvent_Conv = P.PB_GF + V.PARConvGF,
		VtaGPPARObjEvent_Conv = P.PB_GP_PAR + V.PARConvGP,
		VtaFamObjEvent_Jun = P.PB_GF + IIF([Rank] > 90, NULL, V.PARViaNacGF),
		VtaGPPARObjEvent_Jun = P.PB_GP_PAR + IIF([Rank] > 90, NULL, V.PARViaNacGP),
		VtaFamObjEvent_Int = P.PB_GF + V.PARINTLGF,
		VtaFamObjEvent_Gal = P.PB_GF + V.PARGAXSGF,
		VtaFamObjEvent_Gal_Aco = P.PB_GF + V.PARGAXCPF,
		DirPodriaGanar_Fam_Con = IIF(PARConvGF = 0, 0, Num_VtaFam / PARConvGF),
		DirPodriaGanar_GPPAR_Con = IIF(PARConvGP = 0, 0, Num_VtaGPPAR / PARConvGP),
		DirPodriaGanar_Fam_Jun = IIF(IIF([Rank] > 90, NULL, V.PARViaNacGF) = 0, 0, Num_VtaFam / IIF([Rank] > 90, NULL, V.PARViaNacGF)),
		DirPodriaGanar_GPPAR_Jun = IIF(IIF([Rank] > 90, NULL, V.PARViaNacGP) = 0, 0, Num_VtaGPPAR / IIF([Rank] > 90, NULL, V.PARViaNacGP)),
		DirPodriaGanar_Fam_Int = IIF(PARINTLGF = 0, 0, Num_VtaFam / PARINTLGF),
		DirPodriaGanar_Fam_Gal = IIF(PARGAXSGF = 0, 0, Num_VtaFam / PARGAXSGF),
		DirPodriaGanar_Fam_Gal_Aco = IIF(PARGAXCPF = 0, 0, Num_VtaFam / PARGAXCPF)
FROM	#PAR1 P
INNER JOIN Temporary.tmp_ODS_YanbalITSupport_VaraPAR_Local V WITH(NOLOCK)
ON		P.CountryCode = V.CountryCode
AND		P.PARTitle = V.[Rank]
AND		V.Anio = @Year

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 15: UPDATE #PAR1')

UPDATE	P
SET		VtaFamObjCamp_Conv = (VtaFamObjEvent_Conv - VtaTotalAcumFamActualaCX) / C.NroCampaignPARPending,
		VtaGPPARObjCamp_Conv = (VtaGPPARObjEvent_Conv - VtaTotalAcumGrpPerPARActualCX) / C.NroCampaignPARPending,
		VtaFamObjCamp_Jun = (VtaFamObjEvent_Jun - VtaTotalAcumFamActualaCX) / C.NroCampaignPARPending,
		VtaGPPARObjCamp_Jun = (VtaGPPARObjEvent_Jun - VtaTotalAcumGrpPerPARActualCX) / C.NroCampaignPARPending,
		VtaFamObjCamp_Int = (VtaFamObjEvent_Int - VtaTotalAcumFamActualaCX) / C.NroCampaignPARPending,
		VtaFamObjCamp_Gal = (VtaFamObjEvent_Gal - VtaTotalAcumFamActualaCX) / C.NroCampaignPARPending,
		VtaFamObjCamp_Gal_Aco = (VtaFamObjEvent_Gal_Aco - VtaTotalAcumFamActualaCX) / C.NroCampaignPARPending,
		DirPodriaGanar = CASE WHEN DirPodriaGanar_Fam_Gal_Aco > 1 THEN 'Cumb. Mundial c/ A'
								WHEN DirPodriaGanar_Fam_Gal > 1 THEN 'Cumb. Mundial sola'
								WHEN DirPodriaGanar_Fam_Int > 1 THEN 'Viaje Int'
								WHEN DirPodriaGanar_GPPAR_Jun > 1 THEN 'Viaje JNR SEN SSE'
								WHEN DirPodriaGanar_Fam_Jun > 1 THEN 'Viaje JNR SEN SSE'
								WHEN DirPodriaGanar_GPPAR_Con > 1 THEN 'Cumbre Nacional'
								WHEN DirPodriaGanar_Fam_Con > 1 THEN 'Cumbre Nacional'
							ELSE
							'NINGUNO'
							END
FROM	#PAR1 P 
INNER JOIN #CorePeriod C
ON		P.CountryCode = C.CountryCode 

UPDATE	PC
SET		PPEDFaltan = CASE WHEN PARTitle = 70 THEN PAR_JNR_PGFO - FOPARPeriodAccum
							WHEN PARTitle = 80 THEN PAR_SEN_PGFO - FOPARPeriodAccum
							WHEN PARTitle = 90 THEN PAR_SSE_PGFO - FOPARPeriodAccum
							WHEN PARTitle = 100 THEN PAR_REG_PGFO - FOPARPeriodAccum
							WHEN PARTitle = 110 THEN PAR_EST_PGFO - FOPARPeriodAccum
				 			WHEN PARTitle = 120 THEN PAR_MAS_PGFO - FOPARPeriodAccum
							WHEN PARTitle = 130 THEN PAR_EOR_PGFO - FOPARPeriodAccum
							WHEN PARTitle = 140 THEN PAR_EPL_PGFO - FOPARPeriodAccum
							WHEN PARTitle = 150 THEN PAR_EDI_PGFO - FOPARPeriodAccum
							ELSE 0
							END,
		MRM = ISNULL(P.MRM_Amount, 0.00)
FROM	#PAR1 PC
INNER JOIN Sales.ParametersByCampaign P WITH(NOLOCK)
ON		PC.CountryCode = P.CountryCode
AND		PC.YearCampaign = P.Campaign

UPDATE	#PAR1
SET		PPEDFaltan = IIF(PPEDFaltan < 0, 0, PPEDFaltan)

UPDATE	PC
SET		NextMRM = ISNULL(P.MRM_Amount, 0.00)
FROM	#PAR1 PC
INNER JOIN Sales.ParametersByCampaign P WITH(NOLOCK)
ON		P.CountryCode = PC.CountryCode
AND		P.Campaign = PC.YearCampaignAfter

UPDATE	#PAR1
SET		VtaFamObjCamp_Conv = IIF(VtaFamObjCamp_Conv < MRM, NextMRM, VtaFamObjCamp_Conv),
		VtaFamObjCamp_Jun =  IIF(VtaFamObjCamp_Jun < MRM, NextMRM, VtaFamObjCamp_Jun),
		VtaFamObjCamp_Int =  IIF(VtaFamObjCamp_Int < MRM, NextMRM, VtaFamObjCamp_Int),
		VtaFamObjCamp_Gal =  IIF(VtaFamObjCamp_Gal < MRM, NextMRM, VtaFamObjCamp_Gal),
		VtaFamObjCamp_Gal_Aco =  IIF(VtaFamObjCamp_Gal_Aco < MRM, NextMRM, VtaFamObjCamp_Gal_Aco),
		VtaGPPARObjCamp_Conv =  IIF(VtaGPPARObjCamp_Conv < MRM, NextMRM, VtaGPPARObjCamp_Conv),
		VtaGPPARObjCamp_Jun =  IIF(VtaGPPARObjCamp_Jun < MRM, NextMRM, VtaGPPARObjCamp_Jun)

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 16: UPDATE #PAR1')

UPDATE	P 
SET		DelinqencyIdxPGPAR_Rec = I.DelinqencyIdxPGPAR_Rec,
		DelinqencyIdxFamily_Rec = I.DelinqencyIdxFamily_Rec
FROM	#PAR1 P
INNER JOIN Temporary.tmp_ODS_Earnings_PlanDelIndexPARQual_Local I WITH(NOLOCK)
ON		P.CountryCode = I.CountryCode
AND		P.ActivityCenter = I.ActivityCenter
INNER JOIN #PeriodIndexPar PA
ON		PA.CountryCode = I.CountryCode
AND		PA.Id = I.Period

UPDATE	P 
SET		DelinqencyIdxPGPAR = I.DelinqencyIdxPGPAR,
		DelinqencyIdxFamily = I.DelinqencyIdxFamily
FROM	#PAR1 P
INNER JOIN Temporary.tmp_ODS_Earnings_PlanDelIndexQual_Local I WITH(NOLOCK)
ON		P.CountryCode = I.CountryCode
AND		P.ActivityCenter = I.ActivityCenter
INNER JOIN #PeriodIndex PA
ON		PA.CountryCode = I.CountryCode
AND		PA.Id = I.Period

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 17: UPDATE #PAR1')

UPDATE	C
SET		Code = T.code,
		[name] = T.[name],
		InicioPAR = T.InicioPAR,
		VtaTotalAcumFamActualaCX = T.VtaTotalAcumFamActualaCX,
		VtaTotalAcumGrpPerPARActualCX = T.VtaTotalAcumGrpPerPARActualCX,
		VtaFamRealenCX = T.VtaFamRealenCX,
		VntGrpPerPARRealenCX = T.VntGrpPerPARRealenCX,
		VtaFamObjCamp_Conv = T.VtaFamObjCamp_Conv,
		VtaGPPARObjCamp_Conv = T.VtaGPPARObjCamp_Conv,
		VtaFamObjCamp_Jun = T.VtaFamObjCamp_Jun,
		VtaGPPARObjCamp_Jun = T.VtaGPPARObjCamp_Jun,
		VtaFamObjCamp_Int = T.VtaFamObjCamp_Int,
		VtaFamObjCamp_Gal = T.VtaFamObjCamp_Gal,
		VtaFamObjCamp_Gal_Aco = T.VtaFamObjCamp_Gal_Aco,
		DirPodriaGanar = T.DirPodriaGanar,
		FOPARPeriodAccum = T.FOPARPeriodAccum,
		PPEDFaltan = T.PPEDFaltan,
		GVPARPeriodCnt = T.GVPARPeriodCnt,
		NewDDirCnt = T.NewDDirCnt,
		DelinqencyIdxPGPAR = T.DelinqencyIdxPGPAR,
		DelinqencyIdxFamily = T.DelinqencyIdxFamily,
		DelinqencyIdxPGPAR_Rec = T.DelinqencyIdxPGPAR_Rec,
		DelinqencyIdxFamily_Rec = T.DelinqencyIdxFamily_Rec,
		Left_Placement = T.Left_Placement,
		Right_Placement = T.Right_Placement,
		level_placement = T.level_placement,
		CoordinatorCode = T.CoordinatorCode,
		CoordinatorLevel_SalesCoordinator = T.CoordinatorLevel_SalesCoordinator,
		FlagNew = T.FlagNew
FROM	Sales.PARDirectorCampaign C WITH(TABLOCK) 
INNER JOIN #PAR1 T
ON		C.CountryCode = T.CountryCode
AND		C.YearCampaign = T.YearCampaign
AND		C.ActivityCenter = T.ActivityCenter

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 18: UPDATE [Sales].[PARDirectorCampaign]')

INSERT INTO Sales.PARDirectorCampaign WITH(TABLOCK)
SELECT	T.YearCampaign,
		T.CountryCode,
		T.NextCampaign,
		T.ActivityCenter,
		T.code,
		T.[name],
		T.InicioPAR,
		T.VtaTotalAcumFamActualaCX,
		T.VtaTotalAcumGrpPerPARActualCX,
		T.VtaFamRealenCX,
		T.VntGrpPerPARRealenCX,
		T.VtaFamObjCamp_Conv,
		T.VtaGPPARObjCamp_Conv,
		T.VtaFamObjCamp_Jun,
		T.VtaGPPARObjCamp_Jun,
		T.VtaFamObjCamp_Int,
		T.VtaFamObjCamp_Gal,
		T.VtaFamObjCamp_Gal_Aco,
		T.DirPodriaGanar,
		T.FOPARPeriodAccum,
		T.PPEDFaltan,
		T.GVPARPeriodCnt,
		T.NewDDirCnt,
		T.DelinqencyIdxPGPAR,
		T.DelinqencyIdxFamily,
		T.DelinqencyIdxPGPAR_Rec,
		T.DelinqencyIdxFamily_Rec,
		T.Left_Placement,
		T.Right_Placement,
		T.level_placement,
		T.CoordinatorCode,
		T.CoordinatorLevel_SalesCoordinator,
		T.FlagNew
FROM	#PAR1 T
LEFT JOIN Sales.PARDirectorCampaign C WITH(NOLOCK) 
ON		T.CountryCode = C.CountryCode
AND		T.ActivityCenter = C.ActivityCenter
AND		ISNULL(T.YearCampaign, '') = ISNULL(C.YearCampaign, '')
WHERE	C.ActivityCenter IS NULL

INSERT INTO Work.LogExecute VALUES(GETDATE(), @StoreProcedureName, 'PASO 19: FIN')
END
GO

MERGE 