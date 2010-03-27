//http://localhost:5984/funds/_design/shows/_view/fund_relations?startkey=["60000015"]&endkey=["60000015",{}]
//map>>
function(doc) {
	// fundRelation.js
	if (doc.docType == "Fund")
	    emit([doc.LipperId, "0"], true)
	else if (doc.docType == "Asset") 
		emit([doc.LipperId, "asset"], true)
	else if (doc.docType == "AssetStat")
		emit([doc.LipperId, "assetStat"], true)
	else if (doc.docType =="AssetTechnicalAnalysis")
		emit([doc.LipperId, "assetTechAnalysis"], true)
	else if (doc.docType == "FundCompany")
	    emit([doc.LipperId, "company"], true)
	else if (doc.docType == "FundExtra")
		emit([doc.LipperId, "fundExtra"], true)
	else if (doc.docType == "FundLipperScore")
		emit([doc.LipperId, "fundLipperScore"], true)
}
//map<<