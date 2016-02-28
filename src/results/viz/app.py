import json
import web

urls = (
    
    # rest API backend endpoints
    "/rest/static/(.*)", "static_data",
    
    # front-end routes to load angular app
    "/", "index",
    "/(.+)", "www"
)

class www:
    def GET(self, filename):
        try:
            f = open('www/' + filename)
            if filename.endswith(".css"):
                web.header("Content-Type","text/css")
            return f.read() # or return f.read() if you're using 0.3
        except IOError: # No file named that
            web.notfound()
            
class index:
    def GET(self):
        try:
            f = open("www/index.html")
            return f.read()
        except IOError:
            web.notfound()

class static_data:
    def GET(self, name):
        
        # cast string to bool
        def string_to_bool(string):
            return string.lower() in ("yes", "true", "t", "1")
        
        # cast string to float
        def string_to_float(string):
            try:
                digits = float(string)
                return digits
            except:
                return string
        
        # open a csv and convert data for front-end json representation
        def csv_to_json(name):
            try:
            
                # open file
                f = open("../" + name + ".csv")

                # get structure
                lines = f.read().split("\r")
                columns = lines[0].split(",")
                json_obj = []

                # loop through lines
                # skipping first line since it is the csv header row
                iter_lines = iter(lines)
                next(iter_lines)

                for line in iter_lines:

                    # create obj for each line
                    line_obj = {}

                    # get the values from each line
                    line_values = line.split(",")

                    # loop through columns
                    for i, column in enumerate(columns):
                        
                        # check column
                        if (column == "configurable"):
                            
                            # add data to obj
                            # strip out any new lines/carriage returns
                            # cast data types from string csv
                            line_obj[column] = string_to_bool(line_values[i].strip("\r\n"))
                            
                        else:
                            
                            # add data to obj
                            # strip out any new lines/carriage returns
                            # cast data types from string csv
                            line_obj[column] = string_to_float(line_values[i].strip("\r\n"))

                    # add obj to json
                    json_obj.append(line_obj)

                return json_obj

            except IOError:
                web.notfound()
                
        # formatting axis data for drop downs/vizulations
        def configurable_axes(label_array):
            
            options = []
            
            # loop through labels
            for i, label in enumerate(label_array):
                
                # check if configurable
                if (label["configurable"]):
                    
                    # add idx for front-end drop downs
                    label["id"] = i
                    
                    # add to new array
                    options.append(label)
                    
            return options
		
	def nest_json(json_array, nest_key, labels, axis):
		
		# label data
		label_keys = {}
		axis_keys = {}
		
		# loop through labels
		for label in axis:
			axis_keys[label["raw"]] = label["label"]
			
		for label in labels:
			label_keys[label["raw"]] = label["label"] 
					
		# track existing algorithms
		keys = []
		nest_array = []
		
		# loop through objects
		for obj in json_array:
			
			# value of the nested key
			nest_value = obj[nest_key]
			
			# nest obj exists
			if (nest_value in keys):
				
				# get the index
				idx = keys.index(nest_value)
				
				# nest data in existing object
				alg_obj = nest_array[idx]
				
				# empty obj for only the keys we want available to compare
				value_obj = {}
				
				# loop through keys
				for key, value in axis_keys.iteritems():
				
					# get data from nested obj
					value_obj[key] = obj[key]
					
				alg_obj["values"].append(value_obj)
				
			# need a new object
			else:
				
				# create new object
				alg_obj = {}
				value_obj = {}
				
				# loop through keys
				for key, value in axis_keys.iteritems():
				
					# get data from nested obj
					value_obj[key] = obj[key]
						
				alg_obj["key"] = label_keys[nest_value]
				alg_obj["values"] = [value_obj]
				
				# add obj to array
				nest_array.append(alg_obj)
				
				# track it
				keys.append(nest_value)
							
		return nest_array
		
        # set up params
	i = web.input(name=None)
	params = web.input()
	query_term = "structure"
	nest_key = "alg_type"
		
	if (query_term in params):
		data = {}
		data["labels"] = csv_to_json("label_data") # pull label data by default
		data["viz"] = nest_json(csv_to_json(name), nest_key, data["labels"], configurable_axes(data["labels"]))
			
	else:
		data = {}
		data["viz"] = csv_to_json(name)
		data["labels"] = csv_to_json("label_data") # pull label data by default
		data["axisOptions"] = configurable_axes(data["labels"]) # populate axis data objects
        
        return json.dumps(data)
        
app = web.application(urls, globals())
    
if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
