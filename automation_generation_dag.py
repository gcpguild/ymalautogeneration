import sys, re, ruamel.yaml, argparse, json
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import SingleQuotedScalarString as sq
from ruamel.yaml.scalarstring import DoubleQuotedScalarString as dq
from ruamel.yaml.comments import CommentedMap
CS = ruamel.yaml.comments.CommentedSeq  # COMMENTED SEQUENCE
CM = ruamel.yaml.comments.CommentedMap  # COMMENTED MAP
from email_validator import validate_email, EmailNotValidError

yamlschemafile = 'dagschema.yaml'
jsonvariablefile = 'jsonvariable.json'

tagsin = ['Application', 'Main Script', 'PySpark']


arg_def_help_dict = {
        '--e': ('find@ngosys.com', 'Email ID for Airflow Task'),
        '--p': ('gcpguild_project_id', 'GCP Project ID'),
        '--r': ('asia-west3', 'GCP Region'),
        '--c': ('cdp-rubix-dev', 'Composer cluster name'),
        '--t': ( tagsin, 'Tags for DAGs'),
	    '--m': ('pass', 'Actual path of Main Script'),
        '--d': ('App_DAG_ID', 'Dag ID input with underscore. Ex: App_Workflow_DAG_ID'),
        '--i': ('main_task_id', 'Main Task ID input with underscore. Ex: Main_Task_ID'),
        '--s': ('email_task_id', 'Email Task ID input with underscore. Ex: Email_Task_ID'),
        }

parser = argparse.ArgumentParser()

class ArgSwitch:

    def assignval(self, Assign_Values):

        default = "DAG ID"

        return getattr(self, 'variable_' + str(Assign_Values), lambda: default)()

    def variable_e(self):
        return parser.add_argument('--e', default=arg_def_help_dict["--e"][0], help=arg_def_help_dict["--e"][1])
    def variable_p(self):
        return parser.add_argument('--p', default=arg_def_help_dict["--p"][0], help=arg_def_help_dict["--p"][1])
    def variable_r(self):
        return  parser.add_argument('--r', default=arg_def_help_dict["--r"][0], help=arg_def_help_dict["--r"][1])
    def variable_c(self):
        return parser.add_argument('--c', default=arg_def_help_dict["--c"][0], help=arg_def_help_dict["--c"][1])
    def variable_t(self):
         return parser.add_argument('--t', default=arg_def_help_dict["--t"][0], help=arg_def_help_dict["--t"][1],  dest='alist', type=str, nargs='*')
    def variable_m(self):
        return parser.add_argument('--m', default=arg_def_help_dict["--m"][0], help=arg_def_help_dict["--m"][1])
    def variable_i(self):
        return parser.add_argument('--i', default=arg_def_help_dict["--i"][0], help=arg_def_help_dict["--i"][1])
    def variable_s(self):
        return parser.add_argument('--s', default=arg_def_help_dict["--s"][0], help=arg_def_help_dict["--s"][1])
    def variable_d(self):
        return parser.add_argument('--d', default=arg_def_help_dict["--d"][0], help=arg_def_help_dict["--d"][1])

get_val = ArgSwitch()


for k, v in arg_def_help_dict.items():
    get_val.assignval(k[2:])

# parse arguments
args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

#validate email 
def check(email):
	try:
	# validate and get email 
		v = validate_email(email)
		# check email domain
		email = v.normalized
		pass
	except EmailNotValidError as e:
		# email is not valid, exit
		print(str(e))
		exit(1)
                
# checking if valid email ID is provided in argument --e

check(args.e)

Email_ID = args.e

project_id = args.p

region = args.r

cluster_name = args.c 

main_script = args.m 


#----------------------------------------------------------------------------------
def FS1(x):  # flow style list
   res = CS(x)
   res.fa.set_flow_style()
   return res
#----------------------------------------------------------------------------------

def cleanstr(i):
    return re.sub(r'\W+', '_', i)


task_id_main = cleanstr(args.i)

email_status_task_id = cleanstr(args.s)

DAG_ID = cleanstr(args.d) 

dag_real_workjob_name = '{}{}{}{}'.format(DAG_ID.replace('_', ' '), ' ', task_id_main.replace('_', ' '),' ',  ' '.join(tagsin))

json_variable_dict = { 'Email_ID' : args.e,
                      'project_id' : args.p,
                      'region' : args.r,
                      'cluster_name' : args.c,
                      'main_script' : args.m,
                      'task_id_main' : task_id_main,
                      'email_status_task_id' : email_status_task_id,
                      'DAG_ID' : DAG_ID,
                      'dag_real_workjob_name' : dag_real_workjob_name
                      }

default_view = ['tree', 'graph', 'duration', 'gantt', 'landing_times']

orientation = [ 'LR', 'TB', 'RL', 'BT' ]


taskslst = {task_id_main:[{'operator': 'airflow.operators.python.PythonOperator'},
                           {'project_id' : project_id},{'region' : region},
                           {'cluster_name' : cluster_name},
                           {'python_callable' : main_script}]}

emailtask = { email_status_task_id :
            [{'operator' : 'airflow.operators.email_operator.EmailOperator'},
             {'to' :  Email_ID},
             {'subject' : sq('Successfully executed')},
            {'html_content' : sq("The daily scheduled Airflow DAG for the task has completed.")}
            ]}


comtasklst = (taskslst,emailtask)

yaml = YAML(typ='safe', pure=True)
yaml.indent(mapping=2, sequence=4, offset=2)

# - Generatte the output for YMAL
data  = cm = CommentedMap()

deplst = CS()
deplst.append(task_id_main)
depmanot = FS1(deplst)


tagstasklst1 = CS()

for i in range(0, len(tagsin)):
    tagstasklst1.append(dq(tagsin[i]))

manofs = FS1(tagstasklst1)

tagstasklst1.yaml_add_eol_comment("Tags of the tasks", 1, 1)

f = lambda x: str(x).capitalize() if isinstance(x, bool) else  x

app_dict = {
    'default_args' :  [{'owner': sq('airflow')}, 
                            {'start_date': '2023-04-17'},
                            {'retries': 2},
                            {'retry_delay_sec': 120}],
                            'schedule_interval' : sq('0 0 * * *'),
                            'render_template_as_native_obj': f(True),
                            'concurrency': 1,
                            'max_active_runs': 1,
                            'dagrun_timeout_sec': 60,
                            'default_view': 'tree',
                            'orientation': sq(orientation[0]),
                            'default_view':sq(default_view[0]),
                            'description' : sq(dag_real_workjob_name),
                            'tags' : manofs,
                            'tasks' : [taskslst, emailtask],
                            'dependencies' : depmanot
                                                      
}


data[DAG_ID] = app_dict


ruamel.yaml.round_trip_dump(data, sys.stdout, explicit_start=False, default_flow_style=False)


with open('dagschema.yaml', 'w') as fp:
    ruamel.yaml.round_trip_dump(data,fp,  default_flow_style=False, explicit_start=False)


# output as JSON for Airflow Variable Import 
# Airflow --> Admin --> Variable

with open(jsonvariablefile, 'w') as file:
    json_string = json.dumps(json_variable_dict, default=lambda o: o.__dict__, sort_keys=True, indent=2)
    file.write(json_string)
