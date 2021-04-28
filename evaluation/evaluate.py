import argparse
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import os.path
import collections
import math

colors = {'open':'blue',
        'recover':'orange',
        'reopen':'olive',
        'running':'green',
        'commit':'purple',
        'close':'gray',
        'prepare-run':'orange',
        'iterate':'pink',
        }

def renderPhaseChart(data, phase, stats):
  phasedata = data.loc[data['state']==phase,:]
  if len(phasedata) == 0:
    raise Exception("phase '%s' seems invalid (no entries in the stats)"%phase)
  fig = plt.figure(figsize=(15,3*(math.ceil(len(stats)/3)+1)))
  spec = fig.add_gridspec(nrows=1+math.ceil(len(stats)/3.0), ncols=3)
  grp = phasedata.groupby('experiment')
  times = grp.max()['time'] - grp.min()['time']
  durationAx = fig.add_subplot(spec[0,:])
  times.plot.barh(ax=durationAx)
  durationAx.set_title('Duration')
  durationAx.set_xlabel('seconds')

  for i,col in enumerate(stats):
    # get subplot [col, row]
    statAx = fig.add_subplot(spec[int(1+int(i/3)), int(i%3)])
    phasedata.boxplot(column=col.name, showfliers=False,
        by="experiment", ax=statAx)
    if col.ylabel:
      statAx.set_ylabel(col.ylabel)
    if col.title:
      statAx.set_title(col.title)
  fig.suptitle('')
  fig.tight_layout(pad=1.5)
  fig.savefig(phase+".png")

renderCol = collections.namedtuple('column', ['name', 'ylabel', 'title'], defaults=['', ''])

def renderComparisonChart(evaluationFolder, experiments):

  data = None
  for experiment in experiments:
    expData = pd.read_csv(os.path.join(evaluationFolder, experiment, 'stats.csv'))
    expData['experiment'] = experiment
    if data is None:
      data = expData
    else:
      data = data.append(expData)

  data['insertLatMean'] *= 1000000.0
  data['updateLatMean'] *= 1000000.0
  data['readLatMean'] *= 1000000.0

  renderPhaseChart(data, 'recover', [
    renderCol('insertLatMean'),
    renderCol( 'inserts'),
    renderCol(  'writeOnlys'),
      ])
  renderPhaseChart(data, 'running', [ 
   renderCol( 'reads', title='Reads per second (higher is better)'), 
   renderCol( 'inserts', title='Inserts per second (higher is better)'), 
   renderCol( 'updates', title='Updates per second (higher is better)'),
   renderCol( 'readLatMean', ylabel='ms', title='read latency (lower is better)'), 
   renderCol( 'updateLatMean', ylabel='ms', title='update (write) latency (lower is better)'),
   renderCol(  'insertLatMean', ylabel='ms', title='insert (new) latency (lower is better)'),
     ])


def renderChart(evaluationFolder, experiment):
  leveldb = pd.read_csv(os.path.join(evaluationFolder, experiment, 'stats.csv'))

  states = {}

  prevState = ""
  for index, row in leveldb.iterrows():
      state = row['state']
      time = row['time']
      if state not in states:
          states[state] = []
      
      stateList = states[state]

      
      # state did not change, just change the end-time of the last item
      if prevState == state:
          stateList[-1][1] = time
      else:
          # state changed, create a new block
          stateList.append([time, time])
          
      prevState = state
      
  fig, (ax, diskAx, stateAx) = plt.subplots(3, figsize=(10,12),  gridspec_kw={'height_ratios': [20,4,2]})
  memAx = ax.twinx()

  leveldb['mem_mb'] = leveldb['mem'] / (1024*1024)


  stateList = [] 
  colorList = [] 
  stateLabelList = [] 

  for state, stateSlots in states.items():
      for s in stateSlots:
          stateList.append([s[0], s[1]-s[0]])
          colorList.append("tab:"+colors.get(state, 'red'))
          stateLabelList.append(state)
        
      

  ax.set_xlabel('time')
  ax.set_ylabel('operations per second')
  memAx.set_ylabel('used memory in MB')
  leveldb.plot(x='time',
   y=[
    #    'reads', 'updates',  'inserts', 
    'readLatMean', 'updateLatMean', 'insertLatMean',
#    'readOnlys','updateOnlys','writeOnlys',
   ], ax=ax, 
  label=[
#       "Reads per second",
#    "Updates per second",
#    "Inserts per second",
'Read latency (mean)',
'Update latency (mean)',
'Insert latency (mean)',
    # "Reads (if only) per second", 
    #  "Updates (if only) per second",
    #  "Inserts (if only) per second",
     ])
  leveldb.plot(x='time', y='mem_mb', color="purple", ax=memAx)
  ax.legend(loc='upper left')
  ax.set_ylim(0, leveldb['insertLatMean'].quantile(q=0.99)*1.3)
#   ax.set_yscale('log')
  memAx.legend(loc='upper right')

  stateAx.set_ylim(0,16)
  stateAx.set_xlabel('Evaluation Phase')
  stateAx.set_yticks([7])
  stateAx.tick_params('both', width=0,labelsize=0)
  stateAx.broken_barh(stateList, [12,2], facecolors=colorList)

  for pos in ['left', 'right', 'top', 'bottom']:
      stateAx.spines[pos].set_visible(False)

  fig.tight_layout()

  for i, state  in enumerate(stateList):
      stateAx.text(x=state[0] + state[1]/2.0, ha="center", y=1, rotation=45, s=stateLabelList[i])

  fileAx = diskAx.twinx()


  leveldb['disk_mb'] = leveldb['disk'] / (1024*1024)

  leveldb.plot(x='time', y='files', ax=fileAx, color="green", label="files in folder")
  leveldb.plot(x='time', y='disk_mb', ax=diskAx, color="red", label="disk usage")
  fileAx.legend(loc='upper right')
  fileAx.set_ylabel('files in folder')
  diskAx.set_ylabel('disk usage in MB')
  diskAx.legend(loc='upper left')
  diskAx.set_xlabel("Disk & Files")

  fig.savefig(experiment+'.png')

def main(args):

  for result in args.results:
    renderChart(args.folder, result)

  renderComparisonChart(args.folder, args.results)

if __name__=='__main__':
  parser = argparse.ArgumentParser("generator")
  parser.add_argument('--folder', dest='folder')
  parser.add_argument('--results', dest="results", action="append")

  args = parser.parse_args()
  main(args)