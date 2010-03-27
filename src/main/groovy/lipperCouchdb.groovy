import static groovyx.net.http.ContentType.JSON
import static groovyx.net.http.ContentType.HTML

import groovyx.net.http.RESTClient
import groovyx.net.http.HttpResponseException
import groovyx.net.http.HTTPBuilder

import static groovyx.net.http.Method.*
import net.sf.json.JSONObject

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.PutMethod
import org.apache.commons.httpclient.methods.PostMethod
import org.apache.commons.httpclient.methods.StringRequestEntity
import org.apache.commons.httpclient.methods.RequestEntity

import java.text.SimpleDateFormat
/*
 @Grab(group='org.codehaus.groovy.modules.http-builder', module='http-builder', 
 version='0.5.0-RC2')
 def getRESTClient(){
 return new RESTClient("http://localhost:5498/")
 }
 def client = getRESTClient()
 def response = client.put(path: "parking_tickets",
 requestContentType: JSON, contentType: JSON)
 assert response.data.ok == true : "response from server wasn't ok"
 */
class LipperCDB {
	RESTClient client
	def dbname
	
	public LipperCDB(dbname = "funds") {
		this.client = new RESTClient("http://localhost:5984", "$JSON;charset=utf-8")
		this.dbname = dbname
		org.apache.http.params.HttpConnectionParams.setSoTimeout(client.client.params, 5*60000)
	}
	
	def deleteDB() {
		try {
			def resp = client.delete(path: dbname, requestContentType: JSON)
			if (!resp.data.ok) {
				println "Problem deleting the DB: $resp"
			}
		} catch(Exception e) {
			println "Delete Exception: $e"
		}
	}
	
	def createDB() {
		def resp = client.put(path: dbname, requestContentType: JSON)
		//		println "create .... $resp.data"
		if (!resp.data.ok) {
			println "Problem creating the DB: $resp"
		}		
	}
	
	def dbInfo() {
		def resp = client.get(path: dbname, requestContentType: JSON)
		println "create .... $resp.data"
		if (!resp.data.ok) {
			println "Problem creating the DB: $resp"
		}
	}
	
	// curl -X PUT -H "Content-Type: application/json" -d @xx http://localhost:5984/funds/99990008
	// => {"ok":true,"id":"99990008","rev":"1-0fd4883c6c8b736340dd003e5ad296ae"}
	//
	// {"LipperId":"99990008","PrimaryLipperId":"99990008","LaunchDate":"02/01/1996","LaunchCurrencyCode":"USD",
	//  "LaunchPrice":"7.12","LocalCurrencyCode":"USD","DomicileCode":"LUX","GlobalClassCode":"048048",
	//  "LocalClassCode":"048048","GlobalClassBMLipperId":"19048048","LocalClassBMLipperId":"19048048",
	//  "TNADate":"30/03/2007","TNAValueLC":"13.5561","TNAValue":"105.9306","MinInvestDate":"31/01/2007",
	//  "MinInvestInitLC":"1500","MinInvestInit":"11721.35","MinInvestRegLC":"1500","MinInvestReg":"11721.35",
	//  "MinInvestIrregLC":"1500","MinInvestIrreg":"11721.35","MgtChargeDate":"31/01/2007","MgtChargeInitial":"5",
	//  "MgtChargeAnnual":"3","MgtChargeRedemption":"","PriceCode":"N","PriceLC":"15.19","Price":"",
	//  "IncomeDistCode":"R","DivsPerYear":"0"}
	def saveFundxxx(map) {	// This is slow, take 2 sec!!!
		def resp = client.put(path: "$dbname/$map.LipperId",
		//requestContentType: JSON,
		//body: ["LipperId" : map.LipperId])
		body: map)
		println ".....save resp: ${resp.data} in ${System.currentTimeMillis() - startTm}"
		assert resp.data.ok == true, "save fund failed"
	}
	
	HttpClient httpclient = new HttpClient();
	
	def saveFund(map) {
		if (!map)
			return false
		def myjson = new JSONObject();
		myjson.putAll( map );
		
		RequestEntity entity = new StringRequestEntity(myjson.toString(), "application/json", "UTF-8");
		try {
			def method = new PutMethod("http://localhost:5984/$dbname/$map.LipperId")
			method.setRequestEntity(entity)
			def resp = httpclient.executeMethod(method);
		} catch(e) {
			println "saveFund Exception: $e"
		} finally {
			method.releaseConnection();
		}
	}
	
	/*
	 * Bulk load using 
	 * <a href="http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API">Bulk Document API</a>
	 */
	private bulkDocs(docs) {
		def method = new PostMethod("http://localhost:5984/$dbname/_bulk_docs")
		def myjson = new JSONObject();
		myjson.putAll( ["docs": docs] );
		
		RequestEntity entity = new StringRequestEntity(myjson.toString(), "application/json", "UTF-8");
		try {
			method.setRequestEntity(entity)
			def resp = httpclient.executeMethod(method);
			assert resp >= 200 && resp <= 300
		} catch(e) {
			println "buldDocs Exception: $e"
		} finally {
			method.releaseConnection()
		}
	}
	
	// curl -X GET http://localhost:5984/funds/60000008
	def getFund(id) {
		getDoc(id)
	}
	
	/*
	 * get JSON of fund info. See <code>fundRelationsMap</code> below
	 */
	def getFundDetail(id, includeDoc = false) {
		def query = [startkey: "[\"$id\"]", endkey: "[\"$id\",{}]"]
		if (includeDoc)
			query.include_docs = "true"
		try {
			def resp = client.get(path: "$dbname/_design/shows/_view/fund_relations", query: query)
			resp?.data
		} catch(e) {
			println "fundDetail Exception: $e"
		}
	}
	
	/*
	 * Demonstrates the view + list functionality: db/_design/designName/_list/listName/viewName, 
	 * which renders documents to HTML 
	 * <a href="http://wiki.apache.org/couchdb/Formatting_with_Show_and_List">reference</a>
	 */
	def listFundSummary(id, includeDoc = false) {
		def query = [startkey: "[\"$id\"]", endkey: "[\"$id\",{}]"]
 		if (includeDoc)
 			query.include_docs = "true"
 		try {
 			def resp = client.get(path: "$dbname/_design/shows/_list/fund_summary/fund_relations",
 				query: query, requestContentType: HTML, contentType: HTML)
 			resp?.data
 		} catch(e) {
 			println "fundDetail Exception: $e"
 		}
	}
	
	def getDoc(id) {
		def resp = client.get(path: "$dbname/$id")
		assert resp.data?._id == id, "get doc ${id} failed"
		resp?.data
	}
	
	def getDesignDoc(id) {
		try {
			def resp = client.get(path: "$dbname/_design/$id", query: [revs_info: true])
			resp?.data
		} catch(e) {
			println "get design doc ${id} failed"
		}
		//assert resp.data?._id == id, "get design doc ${id} failed"
	}
	
	def deleteDesignDoc(id, doc) {
		def resp = client.delete(path: "$dbname/_design/$id",
			query : [rev: doc._rev])
		assert resp.status == 200, "delete design doc ${doc._id} failed"
		resp?.data
	}
	
	def byDomicile(domicileId) {
		try {
			def resp = client.get(
					path: "$dbname/_design/shows/_view/list_by_DomicileCode",
					query: [key: "\"$domicileId\""]
					)
			resp?.data
		} catch(e) {
			println "byDomicile ($domicileId) Exception: $e"
		}
	}
	
	def withBulk(c) {
		def bd = new BulkDocCollection(this)
		c.call(bd)
		bd.flush()
	}
	
	def doBulkLoad(tabData, idFieldName = "LipperId") {
		withBulk{ b ->
			tabData.eachLine {m ->
				m._id = m."$idFieldName"
				b.add(m)
			}
		}
	}
	
	def doBulkLoadAutoId(tabData) {
		withBulk{ b ->
			tabData.eachLine { m ->
				b.add(m)
			}
		}
	}
	
	@Deprecated
	def multiMerge(tabData) {
		def bulkdocs = new BulkDocCollection(this)
		def currentId
		def map = [:]
		tabData.eachLine { m ->
			currentId = currentId?: m.LipperId
			if (m.LipperId != currentId) {			
				//				map.LipperId = currentId
				bulkdocs.add( mergeMapToDoc(currentId, map, "Company") )
				currentId = m.LipperId
				map = [:]
			}
			map."$m.CompanyRoleCode" = m.CompanyCode
		}
		bulkdocs.flush()
	}
	
	/*
	 * Not generic, only work for FundCompany only as is
	 */
	def bulkLoadMulti(tabData) {
		def currentId
		def map = [:]
		withBulk { b ->
			tabData.eachLine { m ->
				currentId = currentId?: m.LipperId
				if (m.LipperId != currentId) {			
					map.LipperId = currentId
					map.docType = m.docType
					b.add( map)
					currentId = m.LipperId
					map = [:]
				}
				map."$m.CompanyRoleCode" = m.CompanyCode
			}
		}
	}
	
	@Deprecated
	def multiNestMerge(tabData, code) {
		def bulkdocs = new BulkDocCollection(this)
		def currentId
		def map = [:]
		tabData.eachLine { m ->
			currentId = currentId?: m.LipperId
			if (m.LipperId != currentId) {
				bulkdocs.add( mergeMapToDoc(currentId, map, m.docType) )
				currentId = m.LipperId
				map = [:]
			}
			def statCode = m.remove(code)
			m.remove("docType")
			m.remove("LipperId")
			map."$statCode" = m
		}
		bulkdocs.flush()	
	}
	
	def loadMultiAutoId(tabData, code) {
		def currentId
		def map = [:]
		withBulk { b ->
			tabData.eachLine { m ->
				currentId = currentId?: m.LipperId
				if (m.LipperId != currentId) {
					map.docType = m.docType
					map.LipperId = currentId
					b.add( map )
					currentId = m.LipperId
					map = [docType: m.docType]
				}
				def statCode = m.remove(code)
				m.remove("docType")
				m.remove("LipperId")
				map."$statCode" = m
			}
		}
	}
	
	@Deprecated
	private mergeMapToDoc(lipperId, map, fldname) {
		def data
		try {
			data = getFund(lipperId)
		} catch (e) {
			println "failed: $map.LipperId, $e"
			data = [_id: lipperId]
		}
		map.remove("LipperId")
		map.remove("docType")
		data."$fldname" = map
		data
	}
	
	@Deprecated
	def doBulkMerge(tabData) {	// Asset
		def bulkdocs = new BulkDocCollection(this)
		tabData.eachLine { m ->
			bulkdocs.add(mergeMapToDoc(m.LipperId, m, m.docType))
		}
		bulkdocs.flush()
	}
	
	// ========================= Design =======================
	def loadDesign(viewname, contentMap) {
		def oldDoc = getDesignDoc(viewname)
		if (oldDoc)
			deleteDesignDoc(viewname, oldDoc)
		def jsonObj = new JSONObject()
		((JSONObject)jsonObj).putAll( (Map)contentMap )
		RequestEntity entity = new StringRequestEntity(jsonObj.toString(), "application/json", "ISO-8859-1");
		try {
			def method = new PutMethod("http://localhost:5984/$dbname/_design/$viewname")
			method.setRequestEntity(entity)
			HttpClient httpclient = new HttpClient();
			def resp = httpclient.executeMethod(method);
		} finally {
			method.releaseConnection();
		}
	}
	
	def byCompanyCodeMap = """
		function(doc) {
			if (doc.docType == "Company")
				emit(doc.CompanyCode, doc.ShortName)
		}
	"""

	def byTotRet5YrMap = """
		function(doc) {
		  if (doc.docType == "FundLipperScore" && doc.TOTRET5YR)
		    emit(doc.TOTRET5YR.Score, true)
		}
		"""
	/*
	 * This does not work any more
	 */
	def rankByPCT3MGlobalMap = """
		function(doc) {
		  if (doc.docType == "AssetStat" &&  
		    doc.PCT3M && doc.PCT3M.GlobalRank)
		    emit([doc.GlobalClassCode, doc.PCT3M.GlobalRank], true)
		}
	""" 
	
	def summaryShow = """
		function(doc, req) {
		// !json templates.edit
		// !json blog
		// !code vendor/couchapp/path.js
		// !code vendor/couchapp/template.js
		  return doc.LipperId + ":" + doc.LipperId;
}
		"""
	//http://localhost:5984/funds/_design/shows/_list/fund_summary/fund_relations?startkey=["60000009"]&endkey=["60000009",{}]
	def listFundSummary = """
function(head, req) {
	var row;
	while (row = getRow()) {
		if (row.doc) {
			send("<h3>" + row.doc.docType + "</h3>");
		} else {
			send("<p>" + toJSON(row) + "</p>");
		}
	}
}
	"""
	
	def couchViews = "src/main/couchViews/"
	
	def readMRFile = { fname ->
		def ret = [:]
		def f = new File(couchViews + fname)
		def ftext = f.text
		def m = ftext =~ /(?ms)\/\/map>>(.*?)\/\/map<</
		ret.map = m[0][1]
		m = ftext =~ /(?ms)\/\/reduce>>(.*?)\/\/reduce<</
		if (m) {
			ret.reduce = m[0][1]
		}
		ret
	}
	
	def readCountingMRFile = {docType, fname ->
		def map = readMRFile("countingMapReduce.js")
		def binding = [docType: docType, fname: fname]
		def eng = new groovy.text.SimpleTemplateEngine()
		map.map = eng.createTemplate(map.map).make(binding).toString()
		map.reduce = eng.createTemplate(map.reduce).make(binding).toString()
		map
	}
	
	def addCountingViews = {docType, fname, design ->
		def map = readCountingMRFile(docType, fname)
		design.views."count_by_${fname}" = map
		design.views."list_by_${fname}" = [map: map.map]
	}
	
	def designViews() {
		def design = [_id : "_design/domicile", language : "javascript", views : [:], shows : [:], lists: [:]]
		addCountingViews("Fund", "DomicileCode", design)
		addCountingViews("Fund", "GlobalClassCode", design)
		addCountingViews("Fund", "LocalClassCode", design)
		addCountingViews("Fund", "ManagerBMLipperId", design)
		design.views.list_by_companyCode = [map : byCompanyCodeMap]
		design.views.list_by_totRet5Yr = [map: byTotRet5YrMap]
		design.views.fund_relations = readMRFile("fundRelations.js")  //[map: fundRelationsMap]
		design.views.pct1m_by_globalClass = readMRFile("rankAssetStatByGlobalClass.js")
		design.shows.summary = summaryShow
		design.shows.detail = """function(doc, req) {\n return 'Hello World'}\n"""
		design.lists.fund_summary = listFundSummary
		println design
		design
	}
}


class LipperZip {
	def zip
	
	LipperZip(filePath) {
		zip = new java.util.zip.ZipFile(filePath)
	}
	
	def reader(dataFileName) {
		def zipEntry = zip.entries().find{ it.name == dataFileName}
		new BufferedReader(new InputStreamReader(zip.getInputStream(zipEntry)))
	}
	
	def tabData(dataFileName) {
		new TabData(dataFileName, reader(dataFileName))
	}
}

class TabData {
	def name
	def fields
	def br
	def dateFmt = new SimpleDateFormat("yyyy-mm-dd")
	def intFields = ["GlobalRank", "GlobalTotal", "LocalRank", "LocalTotal", 
	"GlobalRankUSD", "GlobalTotalUSD", "LocalRankUSD", "LocalTotalUSD"]
	
	TabData(dataFileName, br) {
		def m = (dataFileName =~ "(.*)\\.txt")
		m.matches()
		this.name = m.group(1)
		this.br = br
		fields = br.readLine().split("\t") as List
		//println "fields ${fields.size} .. $fields"
	}
	
	def nextRecord() {
		def line = br.readLine()
		if (!line)
			return null
		def values = line.split("\t")
		def i = 0
		def m = [:]
		fields.each{ key ->
			if (i < values.length) {
				//println "==> $key : ${values[i]}"
				if (key =~ "Date") {
					if (values[i])
						m."$key" = dateFmt.format(new Date(values[i++]))
					else 
						m."$key" = values[i++]
				} else if (intFields.contains(key)) {
					if (values[i])
						m."$key" = new Integer(values[i++])
					else 
						m."$key" = values[i++]
				} else
					m."$key" = values[i++]
			}
		}
		m.docType = name
		return m
	}
	
	def eachLine(Closure arg) {
		def m
		while(m = nextRecord()) {
			arg.call(m)
		}
	}
}

class BulkDocCollection {
	def docs = []
	def cdb
	def batchSize = 250
	
	BulkDocCollection(cdb) {
		this.cdb = cdb
	}
	
	def add(m) {
		docs.add(m)
		if (docs.size() >= batchSize)
			flush()
	}
	
	def flush() {
		if (docs) {
			cdb.bulkDocs docs
		}
		docs = []
	}
}

/*
 Funds.txt
 - LipperId	
 - PrimaryLipperId	
 - LaunchDate	
 - LaunchCurrencyCode	
 - LaunchPrice	
 - LocalCurrencyCode	
 - DomicileCode	
 - GlobalClassCode	
 - LocalClassCode	
 - GlobalClassBMLipperId	
 - LocalClassBMLipperId	
 - TNADate, TNAValueLC, TNAValue	
 - MinInvestDate, MinInvestInitLC, MinInvestInit, MinInvestRegLC, MinInvestReg, MinInvestIrregLC, MinInvestIrreg
 - MgtChargeDate, MgtChargeInitial, MgtChargeAnnual, MgtChargeRedemption	
 - PriceCode, PriceLC, Price, IncomeDistCode, DivsPerYear	
 - YieldDate, YieldValue, YieldTaxCode
 - IncomeYTM	Income1Y	
 - PriceCode2, PriceLC2, Price2
 ==================================== map example =====================================
 curl -X GET -H "Content-Type: application/json" 
 "http://localhost:5984/funds/_design/LUX/_view/domcile_lux?limit=2&skip=200"
 {"_id":"_design/domiciles","_rev":"1-d8432d48980584c1f131c59e5c2325f7","language":"javascript",
 "views":{
 "count_domiciles":{
 "map":"function(doc) {
 if (doc.DomicileCode != \"\") {  
 emit(doc.DomicileCode, 1);  
 }
 }",
 "reduce":"function(keys, values, rereduce) { return sum(values) }"
 }
 }
 }
 }
 curl -X GET -H "Content-Type: application/json" 
 "http://localhost:5984/funds/_design/domiciles/_view/count_domiciles?startkey=\"HKG\"&endkey=\"IRL\""
 =================== design =============
 curl -X PUT -H "Content-Type: application/json" -d @xml_show.json "http://localhost:5984/sample_db/_design/shows"
 {
 "views": {
 "all": {
 "map": "function(doc) { emit(null, doc); }"
 }
 },
 "lists": {
 "toxml": "Here you inline the show function above.  Make sure all double quotes are escaped as it must be stringified due to the fact that JSON can't store a function type."
 }  
 }
 curl -X PUT -H "Content-Type: application/json" -d @xml_list.json "http://localhost:5984/sample_db/_design/lists"
 point browser to "http://localhost:5984/funds/_design/lists/_view/all"
 curl -X PUT -H 'Content-Type: applicatioe": "javascript",
 "views": {"all":{"map":"function(doc){if(doc.shipment)
 {emit(doc.shipment, null);}}"}}}
 ' http://localhost:5984/hulog_events/_design%2fsendung
 http://localhost:5984/funds/_design/shows/_show/showtest/60000008

delete design document: DELETE' /funds/_design/shows?rev=1-32461b46fb152e513d5f5a2ba14370c7
 */