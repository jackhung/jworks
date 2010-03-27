
//map>>
	function(doc) {
		if (doc.docType == "$docType")
			if (doc.${fname})
				emit(doc.${fname}, 1);
	}
//map<<	
//reduce>>
	function(keys, values, rereduce) {
		return sum(values)
	}
//reduce<<
