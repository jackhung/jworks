import org.openqa.selenium.By
import static org.junit.Assert.*
import static org.junit.matchers.JUnitMatchers.*

this.metaClass.mixin(cuke4duke.GroovyDsl)

Given(~"the zip file '(.*)'") { zipFile ->
  zip = openLipperZip(zipFile)
}

When(~"I open a file with name '(.*)'") { dataFileName ->
  br = zip.reader(dataFileName)
}

Then(~"I should be able to read a data line") {
	line = br.readLine()
}

Then(~"The line should contains '(.*)'") { s ->
  //assertThat(browser.getPageSource(), containsString(text))
  assert line.contains(s)
}

When(~"I get the tabData for '(.*)'") { name ->
	tabData = zip.tabData(name)
}

Then(~"The definition should include field '(.*)'") { name ->
	tabData.fields.contains name
}

Then(~"I should be able to read a record") {
	record = tabData.nextRecord()
}

Then(~"The field '(.*)' should equals '(.*)'") { k, v ->
    //println record
	assert record."$k" == v
}

Given(~"a connection to CouchDB") {
	cdb = new LipperCDB()
}

When(~"I delete and create the funds DB") {
	cdb.deleteDB()
	cdb.createDB()
}

Then(~"I should be able to get the DB info") {
	cdb.dbInfo()
}

Then(~"I load fund records from '(.*)':'(.*)' to the DB") { zipFile, name ->
	zip = openLipperZip(zipFile)
	tabData = zip.tabData(name)
	cdb.doBulkLoad(tabData)
}

Then(~"I should be able to get the fund with LipperId equals '(.*)'") { lipperId ->
	cdb.getFund(lipperId)
}

Then(~"the fund with LipperId equals '(.*)' should have (.*) '(.*)' entries") { lipperId, cntStr, field ->
	fund = cdb.getFund(lipperId)
	assert fund."$field".size() == new Integer(cntStr)
}

Then(~"'(.*)'.'(.*)' should not be empty") { lipperId, fldPath ->
	fund = cdb.getFund(lipperId)
	pathExpr = new GroovyShell().evaluate("c = { f -> f?.${fldPath}}")
	assert pathExpr(fund)
}

Then(~"fund detail '(.*)' should include '(.*)'") { lipperId, key ->
	detail = cdb.getFundDetail(lipperId, true)
	docRow = detail.rows.find{row ->
		row.key[1] == key
	}
	assert docRow.key[0] == lipperId
}

Then(~"fund detail '(.*)':'(.*)':'(.*)' should equals '(.*)'") { lipperId, key, fldPath, value ->
	detail = cdb.getFundDetail(lipperId, true)
	docRow = detail.rows.find{row ->
		row.key[1] == key
	}
	pathExpr = new GroovyShell().evaluate("c = { f -> f?.${fldPath}}")
	assert pathExpr(docRow.doc) == value
}

Then(~"fund detail '(.*)':'(.*)':'(.*)' should not be empty") { lipperId, key, fldPath ->
	detail = cdb.getFundDetail(lipperId, true)
	docRow = detail.rows.find{row ->
		row.key[1] == key
	}
	pathExpr = new GroovyShell().evaluate("c = { f -> f?.${fldPath}}")
	assert pathExpr(docRow.doc)
}

Then(~"'(.*)'.'(.*)' should equals '(.*)'") { lipperId, fldPath, value ->
	fund = cdb.getFund(lipperId)
	pathExpr = new GroovyShell().evaluate("c = { f -> f?.${fldPath}}")
	assert pathExpr(fund) == value
}

When(~"I load a design document") {
	cdb.loadDesign("shows", cdb.designViews())
}

Then(~"I can lookup by domicile '(.*)'") { domicileId ->
	result = cdb.byDomicile(domicileId)	
	println result.rows.size()
	assert result.rows.size() > 0
}

When(~"I merge records from '(.*)':'(.*)' to the DB") { zipFile, name ->
	zip = openLipperZip(zipFile)
	tabData = zip.tabData(name)
	cdb.doBulkMerge(tabData)
}

def openLipperZip(f) {
	new LipperZip("/home/jack/jackworks/lipper/$f")
}

// FundCompany
When(~"I multi-merge records from '(.*)':'(.*)' to the DB") { zipFile, name ->
	zip = openLipperZip(zipFile)
	tabData = zip.tabData(name)
	cdb.bulkLoadMulti(tabData)
}

// Company
When(~"I load records from '(.*)':'(.*)' with id = '(.*)' to the DB") { zipFile, name, idFieldName ->
	zip = openLipperZip(zipFile)
	tabData = zip.tabData(name)
	cdb.doBulkLoad(tabData, idFieldName)
}

Then(~"I load records from '(.*)':'(.*)' to the DB") { zipFile, name ->
	zip = openLipperZip(zipFile)
	tabData = zip.tabData(name)
	cdb.doBulkLoadAutoId(tabData)
}

Then(~"I should be able to get the doc with Id equals '(.*)'") { docId ->
	cdb.getDoc(docId)
}

// AssetStat
When(~"I multi-nest-merge records from '(.*)':'(.*)' using '(.*)' to the DB") { zipFile, name, code ->
	zip = openLipperZip(zipFile)
	tabData = zip.tabData(name)

//	cdb.multiNestMerge(tabData, code)
	cdb.loadMultiAutoId(tabData, code)
}

Then(~"fund '(.*)' relations should include '(.*)' etc.") { lipperId, doclist ->
	docs = doclist.split(",")
	resp = cdb.getFundDetail(lipperId, false)
	docs.each { doc ->
		assert resp.rows.find{ it.key[1] == doc.trim()}
	}	
}

Then(~"list '(.*)' summary should include '(.*)' etc.") { lipperId, doclist ->
	docs = doclist.split(",")
	summary = cdb.listFundSummary(lipperId, true)	// why return json instead if false?
	println summary
	docs.each { doc ->
		// summary isA groovy.util.slurpersupport.NodeChild
		assert summary.text().contains(doc.trim())
	}
}