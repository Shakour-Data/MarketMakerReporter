from RawMaterials.data_base_obj import DataHelper


class IranMarketMakerTableFrameBuilder(DataHelper):
    # Todo: All classes that use dataframe tables must inherit from this class ->1402/08/10 -> 1402/08/30
    def __init__(self):
        super().__init__()

    def build_AnnouncementsInformationTfm(self):
        MarketMakerAnnouncementsInformationTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                            'MarketMakerAnnouncementsInformationTbl',
                                                                            'AnnouncementID')
        return MarketMakerAnnouncementsInformationTfm

    def build_MarketMakerAssetsRayanYekanTfm(self):
        MarketMakerAssetsRayanYekanTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                    'MarketMakerAssetsRayanYekanTbl',
                                                                    'IranCompanyCode12')
        return MarketMakerAssetsRayanYekanTfm

    def build_MarketMakerBasicFundsInformationTfm(self):
        MarketMakerBasicFundsInformationTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                         'MarketMakerBasicFundsInformationTbl',
                                                                         'MarketMakerFundID')
        return MarketMakerBasicFundsInformationTfm

    def build_MarketMakerDailyYekanReportsTfm(self):
        MarketMakerBasicFundsInformationTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                         'MarketMakerDailyYekanReportsTbl',
                                                                         'ReportID')
        return MarketMakerBasicFundsInformationTfm

    def build_MarketMakerFundsFiscalYearYekanTfm(self):
        MarketMakerFundsFiscalYearYekanTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                        'MarketMakerFundsFiscalYearYekanTbl',
                                                                        'FundFiscalYearID')
        return MarketMakerFundsFiscalYearYekanTfm

    def build_MarketMakerInvestorsYekanTfm(self):
        MarketMakerInvestorsYekanTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                  'MarketMakerInvestorsYekanTbl',
                                                                  'InvestorID')
        return MarketMakerInvestorsYekanTfm

    def build_MarketMakerInvestorsFundsTfm(self):
        MarketMakerInvestorsYekanTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                  'MarketMakerInvestorsFundsTbl',
                                                                  'InvestorFundsID')
        return MarketMakerInvestorsYekanTfm

    def build_PreprocessDailyYekanReportTfm(self):
        PreprocessDailyYekanReportTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                  'PreprocessDailyYekanReportTbl',
                                                                  'ReportID')
        return PreprocessDailyYekanReportTfm

    def build_WholeJDateDailyYekanReportHelperTfm(self):
        WholeTimeFramesDailyYekanReportTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                  'BackUpWholeJDateDailyYekanReportHelperTfm',
                                                                  'TimeFrameReportID')
        return WholeTimeFramesDailyYekanReportTfm

    def build_WholeJWeekYekanReportHelperTfm(self):
        WholeTimeFramesWeeklyYekanReportTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                  'BackUpWholeJWeekYekanReportHelperTfm',
                                                                  'TimeFrameReportID')
        return WholeTimeFramesWeeklyYekanReportTfm

    def build_FundsProcessedTfm(self):
        FundsProcessedVfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                  'FundsProcessedVfm',
                                                                  'TimeFrameReportID')
        return FundsProcessedVfm

    # def build_GeneralHoldingsProcessedTfm(self):
    #     GeneralHoldingsProcessedTfm = self.build_table_dataframe('IranMarketMaker.db',
    #                                                               'GeneralHoldingsProcessedVfm',
    #                                                               'ID')
    #
    #     return GeneralHoldingsProcessedTfm

    def build_WordDictTfm(self):
        WordDictTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                        'WordDictTbl',
                                                                        'WordID')
        return WordDictTfm

    def build_MarketMakerHoldingsTfm(self):
        HoldingsTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                        'MarketMakerHoldingsTbl',
                                                                        'HoldingID')

        return HoldingsTfm

    def build_MarketMakerDailyYekanReportsHelperTfm(self):
        MarketMakerDailyYekanReportsHelperTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                           'MarketMakerDailyYekanReportsHelperTbl',
                                                                           'ReportID')

        return MarketMakerDailyYekanReportsHelperTfm

    def build_RawMarketMakerIssuanceCancellationTfm(self):
        IssuanceCancellationTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                        'RawMarketMakerIssuanceCancellationTbl',
                                                                        'ID')

        return IssuanceCancellationTfm

    def build_FundsInvestorsProcessedHelperTfm(self):
        FundsInvestorsHelperTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                        'FundsInvestorsProcessedHelperVfm',
                                                                        'ReportTimeFrameIssuanceCancellationID')

        return FundsInvestorsHelperTfm

    def build_FundsInvestorsProcessedTfm(self):
        FundsInvestorsProcessedTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                        'FundsInvestorsProcessedVfm',
                                                                        'ReportTimeFrameIssuanceCancellationID')

        return FundsInvestorsProcessedTfm

    def build_InvestorsProcessedTfm(self):
        FundsInvestorsProcessedTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                        'InvestorsProcessedVfm',
                                                                        'ID')

        return FundsInvestorsProcessedTfm

    def build_HoldingsProcessedTfm(self):
        HoldingsInvestorsProcessedTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                        'HoldingsProcessedVfm',
                                                                        'ID')

        return HoldingsInvestorsProcessedTfm

    def build_GeneralProcessedTfm(self):
        GeneralInvestorsProcessedTfm = self.build_table_dataframe('IranMarketMaker.db',
                                                                        'GeneralProcessedVfm',
                                                                        'ID')

        return GeneralInvestorsProcessedTfm

class BasicDataBaseTableFrame(DataHelper):
    # Todo: All classes that use dataframe tables must inherit from this class ->1402/08/10 -> 1402/08/30
    def __init__(self):
        super().__init__()

    def build_DateTfm(self):
        DateTfm = self.build_table_dataframe('BasicDataBase.db', 'DateTbl', 'GDate')

        return DateTfm


class IranStockTableFrameBuilder(DataHelper):
    # Todo: All classes that use dataframe tables must inherit from this class ->1402/08/10 -> 1402/08/30
    def __init__(self):
        super().__init__()

    def build_BasicIranIndustriesInformationTfm(self):
        BasicIranIndustriesInformationTfm = self.build_table_dataframe('IranStockDataBase.db',
                                                                       'BasicIranIndustriesInformationTbl',
                                                                       'IndustryIranCode')
        return BasicIranIndustriesInformationTfm

    def build_BasicIranMarketsInformationTfm(self):
        BasicIranMarketsInformationTfm = self.build_table_dataframe('IranStockDataBase.db',
                                                                    'BasicIranMarketsInformationTbl',
                                                                    'IranMarketID')
        return BasicIranMarketsInformationTfm

    def build_BasicIranSubIndustriesInformationTfm(self):
        BasicIranSubIndustriesInformationTfm = self.build_table_dataframe('IranStockDataBase.db',
                                                                          'BasicIranSubIndustriesInformationTbl',
                                                                          'SubIndustryIranCode')
        return BasicIranSubIndustriesInformationTfm

    def build_BasicIranSymbolsInformationTfm(self):
        BasicIranSymbolsInformationTfm = self.build_table_dataframe('IranStockDataBase.db',
                                                                    'BasicIranSymbolsInformationTbl',
                                                                    'IranCompanyCode12')
        return BasicIranSymbolsInformationTfm

    def build_IranStockIntraMarketWatchTfm(self):
        IranStockIntraMarketWatchTfm = self.build_table_dataframe('IranStockDataBase.db',
                                                                  'IranStockIntraMarketWatchTbl',
                                                                  'IntraMarketWatchKey')
        return IranStockIntraMarketWatchTfm

    def build_IranStockIntraOrderBookTfm(self):
        IranStockIntraMarketWatchTfm = self.build_table_dataframe('IranStockDataBase.db',
                                                                  'IranStockIntraOrderBookTblCreator',
                                                                  'IntraMarketWatchKey')
        return IranStockIntraMarketWatchTfm

    def build_IranStockKeyStatesTfm(self):
        IranStockKeyStatesTfm = self.build_table_dataframe('IranStockDataBase.db',
                                                           'IranStockKeyStatesTbl',
                                                           'IntraMarketWatchKey')
        return IranStockKeyStatesTfm

    def build_PreprocessedIranMarketPricesTfm(self):
        PreprocessedIranMarketPricesTfm = self.build_table_dataframe('IranStockDataBase.db',
                                                                     'PreprocessedIranMarketPricesTbl',
                                                                     'PriceKey')
        return PreprocessedIranMarketPricesTfm


class SatesTableFrameBuilder(DataHelper):
    # Todo: All classes that use dataframe tables must inherit from this class ->1402/08/10 -> 1402/08/30
    def __init__(self):
        super().__init__()

    def build_GeneralInstrumentAnalysisStatesTfm(self):
        GeneralInstrumentAnalysisStatesTfm = self.build_table_dataframe('States.db',
                                                                        'GeneralInstrumentAnalysisStatesTbl',
                                                                        'index')
        return GeneralInstrumentAnalysisStatesTfm

    def build_InstrumentPredictStatesTfm(self):
        InstrumentPredictStatesTfm = self.build_table_dataframe('States.db',
                                                                'GeneralInstrumentAnalysisStatesTbl',
                                                                'index')
        return InstrumentPredictStatesTfm

    def build_MarketMakerCashStatesTfm(self):
        MarketMakerCashStatesTfm = self.build_table_dataframe('States.db',
                                                              'MarketMakerCashStatesTbl',
                                                              'index')
        return MarketMakerCashStatesTfm

    def build_MarketMakerNumberOfStockStatesTfm(self):
        MarketMakerNumberOfStockStatesTfm = self.build_table_dataframe('States.db',
                                                                       'MarketMakerNumberOfStockStatesTbl',
                                                                       'index')
        return MarketMakerNumberOfStockStatesTfm

    def build_MarketMakerQuoteStatesTfm(self):
        MarketMakerNumberOfStockStatesTfm = self.build_table_dataframe('States.db',
                                                                       'MarketMakerQuoteStates',
                                                                       'index')
        return MarketMakerNumberOfStockStatesTfm

    def build_MarketMakerNumberOfStockStatesTfm(self):
        MarketMakerNumberOfStockStatesTfm = self.build_table_dataframe('States.db',
                                                                       'MarketMakerStatesTbl',
                                                                       'index')
        return MarketMakerNumberOfStockStatesTfm

    def build_MarketMakerTimeStatesTfm(self):
        MarketMakerTimeStatesTfm = self.build_table_dataframe('States.db',
                                                              'MarketMakerTimeStatesTbl',
                                                              'index')
        return MarketMakerTimeStatesTfm

    def build_PanelAnalysisStateTfm(self):
        PanelAnalysisStateTfm = self.build_table_dataframe('States.db',
                                                           'PanelAnalysisStateTbl',
                                                           'index')
        return PanelAnalysisStateTfm

    def build_PowerOfBSAnalysisStatesTfm(self):
        PowerOfBSAnalysisStatesTfm = self.build_table_dataframe('States.db',
                                                                'PowerOfBSAnalysisStatesTbl',
                                                                'index')
        return PowerOfBSAnalysisStatesTfm

    def build_TransactionStateTfm(self):
        TransactionStateTfm = self.build_table_dataframe('States.db',
                                                         'TransactionStateTbl',
                                                         'index')
        return TransactionStateTfm

    def build_VolumeAnalysisStateTfm(self):
        VolumeAnalysisStateTfm = self.build_table_dataframe('States.db',
                                                            'VolumeAnalysisStateTbl',
                                                            'index')
        return VolumeAnalysisStateTfm