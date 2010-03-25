Feature: Lipper Zip Datasource
  Provide access to zip data

  Scenario: Load design document
    Given a connection to CouchDB
    When I load a design document
    Then I can lookup by domicile 'HKG'
    
  Scenario: Load Asset Data
    Given a connection to CouchDB
    When I load records from 'Zh070411.zip':'Asset.txt' to the DB
    Then fund detail '60000009' should include 'asset'
    And fund detail '60000009':'asset':'ShortName' should equals 'Aberdeen Glo-America Opp B Acc'

  Scenario: Load Score Data
    Given a connection to CouchDB
    When I multi-nest-merge records from 'Zh070411.zip':'FundLipperScore.txt' using 'LipperScoreCode' to the DB
    Then fund detail '60000009' should include 'fundLipperScore'
    And fund detail '60000009':'fundLipperScore':'TOTRET3YR.Score' should not be empty
    
  Scenario: Load FundExtra Data
    Given a connection to CouchDB
    When I load records from 'Zh070411.zip':'FundExtra.txt' to the DB
    Then fund detail '60000009' should include 'fundExtra'
    And fund detail '60000009':'fundExtra':'GeoFocusCode' should equals 'USA'
  
  Scenario: Load Company Data
    Given a connection to CouchDB
    When I load records from 'Zh070411.zip':'Company.txt' with id = 'CompanyCode' to the DB
    Then I should be able to get the doc with Id equals 'AGIC'
    And 'AGIC'.'CompanyCode' should equals 'AGIC'

  Scenario: Load Global Class Data
    Given a connection to CouchDB
    When I load records from 'Zh070411.zip':'GlobalClass.txt' with id = 'GlobalClassCode' to the DB
    Then I should be able to get the doc with Id equals '048048'
    And '048048'.'Name' should equals 'Equity North America'

  Scenario: Load Fund Company Data
    Given a connection to CouchDB
    When I multi-merge records from 'Zh070411.zip':'FundCompany.txt' to the DB
    Then fund detail '60000009' should include 'company'
    And fund detail '60000009':'company':'ADMN' should equals 'BPFS'
#    Then the fund with LipperId equals '60000009' should have 4 'Company' entries
    
  Scenario: Load Asset Stat Data
    Given a connection to CouchDB
    When I multi-nest-merge records from 'Zh070411.zip':'AssetStat.txt' using 'StatCode' to the DB
    Then fund detail '60000009' should include 'assetStat'    
    And fund detail '60000009':'assetStat':'PCTLTM.ValueUSD' should not be empty
    
  Scenario: Load Asset Tech Analysis Data
    Given a connection to CouchDB
    When I multi-nest-merge records from 'Zh070411.zip':'AssetTechnicalAnalysis.txt' using 'StatCode' to the DB
    Then fund detail '60000009' should include 'assetTechAnalysis'
	And fund detail '60000009':'assetTechAnalysis':'SHP6M.ValueUSD' should not be empty
	
  Scenario: Query Fund and related docs
    Given a connection to CouchDB
    Then fund '60000009' relations should include 'asset, assetStat' etc.
    
  Scenario: List Fund summary
    Given a connection to CouchDB
    Then list '60000009' summary should include 'Asset, AssetStat, FundCompany' etc.