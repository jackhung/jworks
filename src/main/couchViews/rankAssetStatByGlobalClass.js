
//map>> 
	function(doc) {
		if (doc.docType == 'AssetStat') {
			emit([doc.LipperId, 1], {pct1mValue: doc.PCT1M.Value})
		} else if (doc.docType == 'Fund') {
			emit([doc.LipperId, 0, doc.GlobalClassCode], true)
		}
	}
//map<<
//reduce>> 
function(keys, values, rereduce) {
		var output = {};
		//var gcCode;
		var pct1m;
		var lipperId;
		var pct1m = {};
		if (rereduce) {
			log (">>>>> ReReduce: cnt=" + values.length + " - " + toJSON(values));
			output.count = 0;
			for (var i = 0; i < values.length; i++) {
				output.count++;
			}
		} else {
			log (">>>>> Reduce: cnt=" + values.length + "\n\t" + toJSON(keys) + "\n\t" + toJSON(values));
			for ( var i = 0; i < values.length; i++) {	// need group_level >= 1
				if (values[i].pct1mValue) {	// from AssetStat doc
					log(keys[i][0] + ".pct1m = " + values[i].pct1mValue);
					if (!pct1m[keys[i][0]]) 
						pct1m[keys[i][0]]= {};
					
					pct1m[keys[i][0]].pct1m = values[i].pct1mValue;	// fundid -> pct1mValue
				} else if (keys[i][1] == 0) {  // from Fund doc
					log(keys[i][0] + ".globalClass = " + keys[i][2]);
					if (!pct1m[keys[i][0]]) 
						pct1m[keys[i][0]]= {};
					pct1m[keys[i][0]].globalClass = keys[i][2];	// fundid -> globalClassCode
				}
			}
		}
		var i = 0
		log(">>>>> PCT1M:" + toJSON(pct1m));
		for ( var p in pct1m) {
			output[p] = pct1m[p];
			if (i++ > 5)
				break;
		}
		log(">>>>> Result: " + toJSON(output));
		return {}; //output;
	}
//reduce<<