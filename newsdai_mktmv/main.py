import sys,os
exec(open('p/newsdai_mktmv.py').read())
from p.pd_proc import *
import traceback
import logging as log
import os.path

from bokeh.layouts import row,column,layout,widgetbox
from bokeh.models import Button,ColumnDataSource,HoverTool,Span
from bokeh.models.widgets import TextInput,DataTable,TableColumn,Div,DateFormatter,NumberFormatter,Paragraph,DatePicker,CheckboxButtonGroup,Select
from bokeh.plotting import figure,curdoc
from datetime import datetime as DT
from datetime import date, timedelta

log.getLogger().setLevel(log.WARN)

# page elements
ww=700
hh=70
searchBox = TextInput(title="filter news",  value='',width=ww,height=hh)
datePick1=DatePicker(title='start date',min_date=date(2005,1,1),max_date=date(2007,12,31),value=date(2007,2,14))
datePick2=DatePicker(title='end date',min_date=date(2005,1,1),max_date=date(2007,12,31),value=date(2007,2,18))
#def setDate(attr,old,new): inputDate=new
#datePick1.on_change('value',setDate)
submit1 = Button(label='Submit',width=20,height=hh)
submit2 = Button(label='Submit',width=20,height=hh)
exeBox  = TextInput(title="function",width=ww,height=hh,css_classes=['itext'])
errBox  = Div(text="",width=ww//3,height=hh,style={'overflow-y':'scroll','height':'150px'})
emptyResult = data=dict(headline=[],prevHeadline=[],symbols=[],gmtstamp=[],ret=[])
searchResultTable = ColumnDataSource(data=emptyResult)
searchResultColumns = [
        TableColumn(field='prevHeadline',title='Past Headline',width=400),
        TableColumn(field='headline',title='Headline',width=400),
        TableColumn(field='sym',title='Symbols',width=70),
        TableColumn(field='gmtstamp',title='GMT Timestamp',formatter=DateFormatter(format="%F %T"),width=180),
        TableColumn(field='date',title='Date',formatter=DateFormatter(format="%Y-%m-%d"),width=120),
        TableColumn(field='ret',title='Daily Return',formatter=NumberFormatter(format='0.00%',text_align='right'),width=80)
        ]
searchResult = DataTable(source=searchResultTable,columns=searchResultColumns,width=2*ww,height=1000)
lineSrc = ColumnDataSource(data={'t':[],'i':[]})
spanSrc = ColumnDataSource(data={'x':[]})
retPlotHover = HoverTool(tooltips=[('headline','@headline')])
retPlot = figure(plot_width=250,plot_height=100,tools=[retPlotHover],x_axis_type='datetime')
solrButton = CheckboxButtonGroup(labels=["solr"], active=[0])
selBox1 = Select(title="Previous Categories:", value="", options=[""])
selBox2 = Select(title="Categories:", value="", options=[""])
if datePick1.value:
    date1 = datePick1.value
    dt1 = DT(date1.year,date1.month,date1.day) - timedelta(days=20)
    retPlot.x_range.start=dt1
if datePick2.value:
    date2 = datePick2.value
    dt2 = DT(date2.year,date2.month,date2.day) + timedelta(days=10)
    retPlot.x_range.end=dt2
retPlot.circle(x='gmtstamp',y='ret',size=7,fill_color='lightskyblue',source=searchResultTable)

# actions
def searchNews1():
    errBox.text = 'Searching...'
    qwords = searchBox.value
    return searchNews(qwords,None)
def searchNews2():
    errBox.text = 'Searching for {}...'.format(searchBox.value)
    qwords = searchBox.value
    qfunction = exeBox.value
    return searchNews(qwords,qfunction)
def searchNews(qwords=None, qfunction=None):
    global lineSrc,spanSrc,retPlot
    searchResultTable.data = emptyResult
    dt1 = datePick1.value
    dt2 = datePick2.value
    Dates,df = [],None
    if dt1: Dates.append(dt1)
    if dt2: Dates.append(dt2)
    try:
        # clear the plot
        errBox.text = ''
        lineSrc.data.update(dict(t=[],i=[]))
        spanSrc.data.update(dict(x=[]))
        mktMove = MktMoveToNews()
        # if query string is not empty and it's not the first time (exeBox is not empty) -> run solr query
        if len(qwords)>0 and solrButton.active:
            df = mktMove.findSolrNews(qwords)
            errBox.text = 'Done'
        else:
            if qfunction:
                if len(qfunction)>0: mktMove.setFunction(qfunction)
            else:
                if len(qwords)>0: mktMove.setKeywords(qwords)
                if len(Dates)>0: mktMove.setDates(Dates)
            df = mktMove.findMktNews()
            exeBox.value = mktMove.getQuery()
            errBox.text = 'Done'
            if solrButton.active:
                if isinstance(df, pd.DataFrame) and not df.empty:
                    errBox.text = 'Now finding categories'
                    mktMove.find_solrClusters()
                    dfClusters = mktMove.getClusters()
                    if len(dfClusters)>0:
                        selBox1.options = [dd['labels'] for dd in dfClusters[0]]
                        selBox2.options = [dd['labels'] for dd in dfClusters[1]]
                    errBox.text = 'Done'
                else:
                    errBox.text = 'Cannot find any news'
            #errBox.text = ', '.join([dd['labels'] for dd in dfClust])
    except Exception as e:
        errBox.text = 'ERROR: '+str(e)
        if log.DEBUG>=log.getLogger().getEffectiveLevel():
            traceback.print_exc()
        return
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        errBox.text = 'Cannot find any news, try again...'
        return
    df['gmtstamp'] = [DT.utcfromtimestamp(ee) for ee in df['gmtstamp']]
    searchResultTable.data = df.to_dict('list')
    minI,maxI = min(indx),max(indx)
    lineSrc = ColumnDataSource(data={'t':ts,'i':indx})
    spanSrc = ColumnDataSource(data={'x':df['gmtstamp'].tolist()})
    retPlot.vbar(x='x',top=maxI,bottom=minI,width=0.1,line_color='lightsteelblue',line_dash='dashed',line_width=1,source=spanSrc)
    retPlot.line(x='t',y='i',line_color='tan',source=lineSrc)
def populateQuery1(attr, old, new):
    searchBox.value = selBox1.value
def populateQuery2(attr, old, new):
    searchBox.value = selBox2.value

search_overlay = row(
    searchBox,
    datePick1,
    column( Div(text="", height=0), submit1)
)

exe_overlay = row(
    exeBox,
    datePick2,
    column( Div(text="", height=0), submit2),
    column( Div(text="", height=0), solrButton)
)

# assemble the page
def assemble_page():
    curdoc().clear()
    curdoc().add_root(layout([[search_overlay], \
        [exe_overlay],\
        [errBox], \
        [selBox1, selBox2], \
        [retPlot], \
        [searchResult]],sizing_mode='scale_width'))
    curdoc().title = "Newsdai"


ts,indx=None,None
qqqPath='data/out'
try:
    import pandas as pd
    tsCache = qqqPath+'/qqqTs.pkl'
    indxCache = qqqPath+'/qqqIndx.pkl'
    if os.path.isfile(tsCache) and os.path.isfile(indxCache):
        ts = pd.read_pickle(tsCache)
        indx = pd.read_pickle(indxCache)
    else:
        ts,indx=qqq_ret(getRetDF())
        ts.to_pickle(tsCache)
        indx.to_pickle(indxCache)
    if not ts.empty and not indx.empty:
      try:
        lineSrc = ColumnDataSource(data={'t':ts,'i':indx})
        retPlot.line(x='t',y='i',line_color='tan',source=lineSrc)
      except Exception as e:
        errBox.text = 'ERROR: '+str(e)
        if log.DEBUG>=log.getLogger().getEffectiveLevel():
            traceback.print_exc()
    else:
        errBox.text = 'ERROR: QQQ timeseries is emtpy'
except Exception as e:
    errBox.text = 'ERROR: '+str(e)
    if log.DEBUG>=log.getLogger().getEffectiveLevel():
        traceback.print_exc()

# assign actions
submit1.on_click(searchNews1)
submit2.on_click(searchNews2)
selBox1.on_change('value', populateQuery1)
selBox2.on_change('value', populateQuery2)
assemble_page()
#if log.DEBUG>=log.getLogger().getEffectiveLevel(): searchNews("CEO",None)
