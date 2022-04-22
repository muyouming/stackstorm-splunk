from pyrfc import Connection
import pandas as pd

from st2common.runners.base_action import Action

__all__ = [
    'GetSAPTable'
]


class GetSAPTable(Action):

    def __init__(self, config):
        super(GetSAPTable, self).__init__(config)
        # Validate config is set
        if config is None:
            raise ValueError("No sap configuration details found")
        if "sap_instances" in config:
            if config['sap_instances'] is None:
                raise ValueError("'sap_instances' config defined but empty.")
            else:
                pass
        else:
            raise ValueError("No sap configuration details found")



    def run(self,instance,username,password, Fields, SQLTable, Where = '', MaxRows=50, FromRow=0):
        j = '[]'

        if instance:
            sap_config = self.config['sap_instances'].get(instance)
        else:
            sap_config = self.config['sap_instances'].get('default')


        try:
            self.conn = Connection(ashost=sap_config.get('host'),
                                    sysnr=sap_config.get('sysnr'),#'00', 
                                    client=sap_config.get('client'),#302, 
                                    user=username, 
                                    passwd=password)


            """A function to query SAP with RFC_READ_TABLE"""


            # By default, if you send a blank value for fields, you get all of them
            # Therefore, we add a select all option, to better mimic SQL.
            if Fields[0] == '*':
                Fields = ''
            else:
                Fields = [{'FIELDNAME':x} for x in Fields] # Notice the format


            # the WHERE part of the query is called "options"
            options = [{'TEXT': x} for x in Where] # again, notice the format


            # we set a maximum number of rows to return, because it's easy to do and
            # greatly speeds up testing queries.
            rowcount = MaxRows


            # Here is the call to SAP's RFC_READ_TABLE
            tables = self.conn.call("RFC_READ_TABLE", QUERY_TABLE=SQLTable, DELIMITER='|', FIELDS = Fields,
                                    OPTIONS=options, ROWCOUNT = MaxRows, ROWSKIPS=FromRow)


            # We split out fields and fields_name to hold the data and the column names
            fields = []
            fields_name = []


            data_fields = tables["DATA"] # pull the data part of the result set
            data_names = tables["FIELDS"] # pull the field name part of the result set


            headers = [x['FIELDNAME'] for x in data_names] # headers extraction
            long_fields = len(data_fields) # data extraction
            long_names = len(data_names) # full headers extraction if you want it


            # now parse the data fields into a list
            for line in range(0, long_fields):
                fields.append(data_fields[line]["WA"].strip())


            # for each line, split the list by the '|' separator
            values = [x.strip().split('|') for x in fields ]
            
            d = pd.DataFrame.from_records(values,columns=headers)
            d = d.applymap(lambda x:x.strip())
            j = d.to_json(orient='records')
            # return the 2D list and the headers
        except Exception as e:
            print(e)
        
        return j
