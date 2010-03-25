import net.sf.json.JSONObject
import groovyx.net.http.RESTClient
import static groovyx.net.http.ContentType.JSON

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.PutMethod
import org.apache.commons.httpclient.methods.StringRequestEntity
import org.apache.commons.httpclient.methods.RequestEntity

@Grab(group='org.codehaus.groovy.modules.http-builder', module='http-builder', version='0.5.0-RC3')
@Grab(group="net.sf.json-lib", module="json-lib", version="2.3", classifier='jdk15')
@Grab(group="commons-httpclient", module="commons-httpclient", version="3.1")

def design = [:]
design._id = "_design/domicile"
design.language = "javascript"
design.views = [:]
design.views.all = [:]
design.views.all.map = """
	function(doc) {
		emit(doc.DomicileCode, 1);
	}
	"""
design.views.all.reduce = """
	function(keys, values, rereduce) {
		return sum(values)
	}
	"""
design.views.by_domicile = [:]
design.views.by_domicile.map = """
	function(doc) {
		if (doc.Type == 'funds')  
			emit(doc.DomicileId, doc) 
	}
	"""


def jsonObj = new JSONObject()
((JSONObject)jsonObj).putAll( (Map)design )
println jsonObj.toString()

//def client = new RESTClient("http://localhost:5984", JSON)
//
//def designjson = client.getEncoder().encodeJSON(design)
//
//println designjson.toString()

RequestEntity entity = new StringRequestEntity(jsonObj.toString(), "application/json", "ISO-8859-1");
try {
	def method = new PutMethod("http://localhost:5984/funds/_design/shows")
	method.setRequestEntity(entity)
	HttpClient httpclient = new HttpClient();
	def resp = httpclient.executeMethod(method);
	println resp
} finally {
	// Release current connection to the connection pool once you are
	// done
	method.releaseConnection();
}
