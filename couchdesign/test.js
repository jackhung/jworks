{
	"_id" : "_design/domiciles",
	"_rev" : "1-d8432d48980584c1f131c59e5c2325f7",
	"language" : "javascript",
	"views" : {
		"count_domiciles" : {
			"map" : "function(doc) {\n  if (doc.DomicileCode != \"\") {\n  emit(doc.DomicileCode, 1);\n  }\n}",
			"reduce" : "function(keys, values, rereduce) {\n  return sum(values)\n}"
		}
	}
}