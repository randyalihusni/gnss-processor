"""
GNSS Processing Report Generator v2
=====================================
Pages:
  1. Cover + Processing Info + Coordinate Results + Location Map
  2. Position Time Series + Precision Analysis
  3. Solution Quality Detail (Q-distribution, PDOP, satellite count)
  4. LC/LW Linear Combination Analysis
"""
import os, io, math
from datetime import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
import matplotlib.ticker as ticker
import numpy as np

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    Image, HRFlowable, PageBreak, KeepTogether
)
from reportlab.platypus.flowables import Flowable
from reportlab.lib.colors import HexColor

# ── Colors ───────────────────────────────────────────────────────────────────
C_DARK   = HexColor('#0a1628')
C_PANEL  = HexColor('#0d2240')
C_ACCENT = HexColor('#00c49a')
C_ACC2   = HexColor('#0077e6')
C_WARN   = HexColor('#e09500')
C_LIGHT  = HexColor('#e8f2ff')
C_DIM    = HexColor('#4a6080')
C_MID    = HexColor('#8aa0b8')
C_GOOD   = HexColor('#00b87a')
C_BAD    = HexColor('#e04444')
C_TBLALT = HexColor('#f0f6ff')
C_WHITE  = colors.white

MPL_STYLE = {
    'figure.facecolor':'#0d1b2e','axes.facecolor':'#111e30',
    'axes.edgecolor':'#1e3050','axes.labelcolor':'#8aa0b8',
    'axes.grid':True,'grid.color':'#1e3050','grid.linewidth':0.6,
    'xtick.color':'#4a6080','ytick.color':'#4a6080',
    'text.color':'#c0d4e8','font.family':'monospace','font.size':9,
}

# ── Custom Flowables ──────────────────────────────────────────────────────────
class SectionHeader(Flowable):
    def __init__(self, label, title, width=None, accent=C_ACCENT):
        super().__init__()
        self._label=label; self._title=title
        self._width=width or (A4[0]-40*mm); self._accent=accent; self.height=22
    def draw(self):
        c=self.canv; w=self._width
        c.setFillColor(C_PANEL); c.roundRect(0,0,w,self.height,2,fill=1,stroke=0)
        c.setFillColor(self._accent); c.rect(0,0,3,self.height,fill=1,stroke=0)
        c.setFillColor(self._accent); c.setFillAlpha(0.15)
        c.roundRect(8,4,28,14,2,fill=1,stroke=0); c.setFillAlpha(1)
        c.setFillColor(self._accent); c.setFont('Courier-Bold',8); c.drawString(12,8,self._label)
        c.setFillColor(C_LIGHT); c.setFont('Courier-Bold',9); c.drawString(44,8,self._title.upper())

class ThinRule(Flowable):
    def __init__(self, width=None, color=C_DIM, thickness=0.5):
        super().__init__()
        self._width=width or (A4[0]-40*mm); self._color=color; self._thickness=thickness; self.height=1
    def draw(self):
        self.canv.setStrokeColor(self._color); self.canv.setLineWidth(self._thickness)
        self.canv.line(0,0,self._width,0)

def _on_page(canvas, doc, job):
    W,H=A4; canvas.saveState()
    canvas.setFillColor(C_DARK); canvas.rect(0,H-18*mm,W,18*mm,fill=1,stroke=0)
    canvas.setFillColor(C_ACCENT); canvas.rect(0,H-18*mm,5,18*mm,fill=1,stroke=0)
    canvas.setFillColor(C_LIGHT); canvas.setFont('Courier-Bold',9)
    canvas.drawString(12*mm,H-9*mm,'GNSS POST-PROCESSING REPORT')
    canvas.setFillColor(C_MID); canvas.setFont('Courier',7)
    canvas.drawRightString(W-15*mm,H-9*mm,f"JOB #{job.get('id','—')}  |  {datetime.now().strftime('%Y-%m-%d')}")
    canvas.setFillColor(C_DARK); canvas.rect(0,0,W,12*mm,fill=1,stroke=0)
    canvas.setFillColor(C_ACCENT); canvas.rect(0,0,W,1.5,fill=1,stroke=0)
    canvas.setFillColor(C_MID); canvas.setFont('Courier',7)
    canvas.drawString(15*mm,4*mm,'KJSB Randy dan Rekan — Geodetic Survey Division')
    canvas.drawRightString(W-15*mm,4*mm,f'Page {doc.page}')
    canvas.restoreState()

def _styles():
    s=getSampleStyleSheet()
    def add(name,**kw): s.add(ParagraphStyle(name=name,**kw))
    add('GT', fontName='Courier-Bold',fontSize=22,textColor=C_LIGHT,spaceAfter=4,leading=28)
    add('GS', fontName='Courier',fontSize=10,textColor=C_ACCENT,spaceAfter=2,leading=14)
    add('GL', fontName='Courier-Bold',fontSize=8,textColor=C_ACCENT,spaceAfter=2,leading=12)
    add('GV', fontName='Courier',fontSize=9,textColor=C_LIGHT,spaceAfter=2,leading=13)
    add('GC', fontName='Courier',fontSize=7,textColor=C_DIM,spaceAfter=6,leading=10,alignment=TA_CENTER)
    add('GN', fontName='Courier',fontSize=7.5,textColor=C_MID,spaceAfter=4,leading=11)
    add('GB', fontName='Courier',fontSize=8.5,textColor=C_LIGHT,spaceAfter=4,leading=13)
    add('GI', fontName='Courier',fontSize=8,textColor=C_MID,spaceAfter=4,leading=12)
    return s

def _ts(extra=None):
    base=[
        ('FONTNAME',(0,0),(-1,-1),'Courier'),('FONTSIZE',(0,0),(-1,-1),8),
        ('FONTNAME',(0,0),(-1,0),'Courier-Bold'),('BACKGROUND',(0,0),(-1,0),C_PANEL),
        ('TEXTCOLOR',(0,0),(-1,0),C_ACCENT),('FONTSIZE',(0,0),(-1,0),8),
        ('ROWBACKGROUNDS',(0,1),(-1,-1),[C_WHITE,C_TBLALT]),
        ('TEXTCOLOR',(0,1),(-1,-1),C_DARK),
        ('GRID',(0,0),(-1,-1),0.3,C_DIM),
        ('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),
        ('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
    ]
    if extra: base+=extra
    return base

def _prec(std_m):
    mm=std_m*1000
    if mm<1: return 'Sub-mm (<1mm)'
    if mm<5: return f'mm-level ({mm:.1f}mm)'
    if mm<20: return f'cm-level ({mm:.0f}mm)'
    return f'Coarse (>{mm:.0f}mm)'

def _pc(std_m):
    mm=std_m*1000
    return C_GOOD if mm<5 else C_WARN if mm<20 else C_BAD

# ── Plots ─────────────────────────────────────────────────────────────────────
def _plot_timeseries(epochs_arr, summary):
    lat0=summary['lat_mean']; lon0=summary['lon_mean']; ht0=summary['height_mean']
    cos_lat=math.cos(math.radians(lat0))
    lats=np.array([e.lat    for e in epochs_arr])
    lons=np.array([e.lon    for e in epochs_arr])
    hts =np.array([e.height for e in epochs_arr])
    Q   =np.array([e.Q      for e in epochs_arr])
    lat_m=(lats-lat0)*111000; lon_m=(lons-lon0)*111000*cos_lat; ht_m=hts-ht0
    n=len(epochs_arr); t=np.arange(n)
    with plt.style.context(MPL_STYLE):
        fig=plt.figure(figsize=(9,5.5),facecolor=MPL_STYLE['figure.facecolor'])
        gs=gridspec.GridSpec(3,1,hspace=0.06,figure=fig)
        axes=[fig.add_subplot(gs[i]) for i in range(3)]
        series=[(lat_m,'#00c49a','dLat (m)'),(lon_m,'#0099ff','dLon (m)'),(ht_m,'#f0a500','dHt (m)')]
        fm=Q==1; fl=Q==2; fs=~(fm|fl)
        for ax,(vals,col,ylabel) in zip(axes,series):
            if fs.any(): ax.scatter(t[fs],vals[fs],s=0.4,c='#e04444',alpha=0.4,linewidths=0)
            if fl.any(): ax.scatter(t[fl],vals[fl],s=0.5,c='#ffaa00',alpha=0.5,linewidths=0)
            if fm.any(): ax.scatter(t[fm],vals[fm],s=0.5,c=col,alpha=0.6,linewidths=0)
            sd=np.std(vals)
            ax.axhline(0,color=col,lw=0.8,alpha=0.5)
            ax.axhline(3*sd,color=col,lw=0.5,ls='--',alpha=0.3)
            ax.axhline(-3*sd,color=col,lw=0.5,ls='--',alpha=0.3)
            ax.set_ylabel(ylabel,fontsize=8,color=col,labelpad=4)
            ax.set_xlim(0,n); ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.4f'))
            if ax!=axes[-1]: ax.set_xticklabels([])
            else: ax.set_xlabel('Epoch',fontsize=8,labelpad=4)
            for sp in ax.spines.values(): sp.set_edgecolor('#1e3050')
        legend_els=[mpatches.Patch(color='#00c49a',label='Fixed (Q=1)'),
                    mpatches.Patch(color='#ffaa00',label='Float (Q=2)'),
                    mpatches.Patch(color='#e04444',label='Single/Other')]
        axes[0].legend(handles=legend_els,loc='upper right',fontsize=7,
                       framealpha=0.3,facecolor='#0d1b2e',edgecolor='#1e3050')
        fig.suptitle('Position Deviation from Mean (per Epoch)',fontsize=10,color='#c0d4e8',y=0.98)
    buf=io.BytesIO(); fig.savefig(buf,format='png',dpi=150,bbox_inches='tight',facecolor=fig.get_facecolor()); plt.close(fig); buf.seek(0); return buf

def _plot_scatter(epochs_arr, summary):
    lat0=summary['lat_mean']; lon0=summary['lon_mean']; ht0=summary['height_mean']
    cos_lat=math.cos(math.radians(lat0))
    lat_m=(np.array([e.lat    for e in epochs_arr])-lat0)*111000
    lon_m=(np.array([e.lon    for e in epochs_arr])-lon0)*111000*cos_lat
    ht_m =np.array([e.height for e in epochs_arr])-ht0
    Q=np.array([e.Q for e in epochs_arr])
    cols=np.where(Q==1,'#00c49a',np.where(Q==2,'#ffaa00','#e04444'))
    ls=summary['lat_std']; lns=summary['lon_std']
    with plt.style.context(MPL_STYLE):
        fig=plt.figure(figsize=(9,4),facecolor=MPL_STYLE['figure.facecolor'])
        gs=gridspec.GridSpec(1,3,figure=fig,wspace=0.35)
        ax_ne=fig.add_subplot(gs[0:2]); ax_ht=fig.add_subplot(gs[2])
        ax_ne.scatter(lon_m,lat_m,s=0.8,c=cols,alpha=0.5,linewidths=0)
        ax_ne.axhline(0,color='#8aa0b8',lw=0.5,ls='--'); ax_ne.axvline(0,color='#8aa0b8',lw=0.5,ls='--')
        cep=0.59*(ls+lns)
        circle=plt.Circle((0,0),cep,fill=False,color='#00c49a',lw=0.8,ls=':',alpha=0.7); ax_ne.add_patch(circle)
        ax_ne.text(cep*0.7,cep*0.7,f'CEP\n{cep*1000:.1f}mm',fontsize=7,color='#00c49a',alpha=0.9)
        ax_ne.set_xlabel('dEast (m)',fontsize=8); ax_ne.set_ylabel('dNorth (m)',fontsize=8)
        ax_ne.set_title('N/E Position Scatter',fontsize=9,color='#c0d4e8'); ax_ne.set_aspect('equal')
        for sp in ax_ne.spines.values(): sp.set_edgecolor('#1e3050')
        ax_ht.hist(ht_m,bins=50,orientation='horizontal',color='#f0a500',alpha=0.6,edgecolor='none')
        ax_ht.axhline(0,color='#8aa0b8',lw=0.6,ls='--')
        ax_ht.set_xlabel('Count',fontsize=8); ax_ht.set_ylabel('dHeight (m)',fontsize=8)
        ax_ht.set_title('Height Distribution',fontsize=9,color='#c0d4e8')
        for sp in ax_ht.spines.values(): sp.set_edgecolor('#1e3050')
    buf=io.BytesIO(); fig.savefig(buf,format='png',dpi=150,bbox_inches='tight',facecolor=fig.get_facecolor()); plt.close(fig); buf.seek(0); return buf

def _plot_map(lat, lon, radius_m=500):
    r_lat=radius_m/111000; r_lon=radius_m/(111000*math.cos(math.radians(lat)))
    with plt.style.context(MPL_STYLE):
        fig,ax=plt.subplots(figsize=(5,4),facecolor=MPL_STYLE['figure.facecolor'])
        ax.set_facecolor('#0a1628')
        for i in range(-4,5):
            ax.axhline(lat+i*r_lat/4,color='#1e3050',lw=0.4)
            ax.axvline(lon+i*r_lon/4,color='#1e3050',lw=0.4)
        for frac,alpha in [(0.3,0.5),(0.6,0.35),(1.0,0.25)]:
            ax.add_patch(plt.Circle((lon,lat),r_lon*frac,fill=False,color='#00c49a',lw=0.8,ls='--',alpha=alpha))
        ax.plot(lon,lat,'o',markersize=8,color='#00c49a',markeredgecolor='white',markeredgewidth=0.8,zorder=5)
        ax.plot([lon-r_lon,lon+r_lon],[lat,lat],color='#00c49a',lw=0.6,alpha=0.5)
        ax.plot([lon,lon],[lat-r_lat,lat+r_lat],color='#00c49a',lw=0.6,alpha=0.5)
        ax.annotate(f'  {lat:.6f}°N\n  {lon:.6f}°E',xy=(lon,lat),
                    xytext=(lon+r_lon*0.15,lat+r_lat*0.15),fontsize=7.5,color='#c0d4e8',fontfamily='monospace')
        ax.set_xlim(lon-r_lon,lon+r_lon); ax.set_ylim(lat-r_lat,lat+r_lat)
        ax.set_xlabel('Longitude',fontsize=8); ax.set_ylabel('Latitude',fontsize=8)
        ax.set_title('Point Location',fontsize=9,color='#c0d4e8')
        ax.xaxis.set_major_formatter(ticker.FormatStrFormatter('%.5f'))
        ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.5f'))
        for sp in ax.spines.values(): sp.set_edgecolor('#1e3050')
    buf=io.BytesIO(); fig.savefig(buf,format='png',dpi=150,bbox_inches='tight',facecolor=fig.get_facecolor()); plt.close(fig); buf.seek(0); return buf

def _plot_solution_quality(sol):
    """Page 3: Q-distribution pie + satellite count + PDOP bar"""
    with plt.style.context(MPL_STYLE):
        fig=plt.figure(figsize=(9,4.5),facecolor=MPL_STYLE['figure.facecolor'])
        gs=gridspec.GridSpec(1,3,figure=fig,wspace=0.4)

        # 1) Q distribution pie
        ax1=fig.add_subplot(gs[0])
        labels=[]; sizes=[]; pie_cols=[]
        for label,n,col in [
            ('Fixed',   sol.n_fixed,   '#00c49a'),
            ('Float',   sol.n_float,   '#0099ff'),
            ('DGPS',    sol.n_dgps,    '#aa44ff'),
            ('SBAS',    sol.n_sbas,    '#ff8800'),
            ('Single',  sol.n_single,  '#e04444'),
            ('PPP',     sol.n_ppp,     '#ffdd00'),
        ]:
            if n>0: labels.append(f'{label}\n{n}'); sizes.append(n); pie_cols.append(col)
        if sizes:
            wedges,texts,autotexts=ax1.pie(sizes,labels=labels,colors=pie_cols,
                autopct='%1.1f%%',startangle=90,
                textprops={'fontsize':7,'color':'#c0d4e8'},
                wedgeprops={'edgecolor':'#0d1b2e','linewidth':1})
            for at in autotexts: at.set_fontsize(6); at.set_color('#0d1b2e')
        ax1.set_title('Solution Quality\nDistribution',fontsize=9,color='#c0d4e8')

        # 2) Satellite count time series (sampled)
        ax2=fig.add_subplot(gs[1])
        n_ep=len(sol.epochs)
        step=max(1,n_ep//500)
        t_s=np.arange(0,n_ep,step)
        ns_s=np.array([sol.epochs[i].ns for i in t_s])
        ax2.fill_between(t_s,ns_s,alpha=0.5,color='#0099ff')
        ax2.plot(t_s,ns_s,lw=0.5,color='#00c49a')
        ax2.axhline(sol.ns_mean,color='#ffdd00',lw=1,ls='--',alpha=0.8,label=f'Mean={sol.ns_mean:.1f}')
        ax2.set_xlabel('Epoch',fontsize=8); ax2.set_ylabel('# Satellites',fontsize=8)
        ax2.set_title('Satellite Count',fontsize=9,color='#c0d4e8')
        ax2.legend(fontsize=7,framealpha=0.3,facecolor='#0d1b2e',edgecolor='#1e3050')
        ax2.set_xlim(0,n_ep); ax2.set_ylim(0,sol.ns_max+2)
        for sp in ax2.spines.values(): sp.set_edgecolor('#1e3050')

        # 3) Q per epoch (colored bar — sampled)
        ax3=fig.add_subplot(gs[2])
        Q_s=np.array([sol.epochs[i].Q for i in t_s])
        q_cols=np.where(Q_s==1,'#00c49a',np.where(Q_s==2,'#0099ff',np.where(Q_s==5,'#e04444','#ff8800')))
        ax3.bar(t_s,np.ones(len(t_s)),width=step*1.2,color=q_cols,alpha=0.85,linewidth=0)
        ax3.set_xlabel('Epoch',fontsize=8); ax3.set_yticks([])
        ax3.set_title('Solution Type\nper Epoch',fontsize=9,color='#c0d4e8')
        ax3.set_xlim(0,n_ep)
        legend_els=[mpatches.Patch(color='#00c49a',label='Fixed'),
                    mpatches.Patch(color='#0099ff',label='Float'),
                    mpatches.Patch(color='#e04444',label='Single'),
                    mpatches.Patch(color='#ff8800',label='SBAS/DGPS')]
        ax3.legend(handles=legend_els,fontsize=6,framealpha=0.3,
                   facecolor='#0d1b2e',edgecolor='#1e3050',ncol=2)
        for sp in ax3.spines.values(): sp.set_edgecolor('#1e3050')

        fig.suptitle('Solution Quality Analysis',fontsize=10,color='#c0d4e8',y=0.98)
    buf=io.BytesIO(); fig.savefig(buf,format='png',dpi=150,bbox_inches='tight',facecolor=fig.get_facecolor()); plt.close(fig); buf.seek(0); return buf

def _plot_lc_bars(lc_info):
    """Page 4: LC/LW RMS bar chart"""
    names=[lc['name'].split('(')[0].strip() for lc in lc_info]
    rms  =[lc['rms_mm']  for lc in lc_info]
    std  =[lc['std_mm']  for lc in lc_info]
    slip =[lc['n_slip']  for lc in lc_info]
    cols =['#00c49a','#0099ff','#aa44ff','#ff8800','#e04444'][:len(names)]
    with plt.style.context(MPL_STYLE):
        fig=plt.figure(figsize=(9,3.5),facecolor=MPL_STYLE['figure.facecolor'])
        gs=gridspec.GridSpec(1,2,figure=fig,wspace=0.35)

        # RMS bars
        ax1=fig.add_subplot(gs[0])
        x=np.arange(len(names))
        bars=ax1.bar(x,rms,color=cols,alpha=0.75,edgecolor='#0d1b2e',linewidth=0.5)
        ax1.errorbar(x,rms,yerr=std,fmt='none',color='white',capsize=4,linewidth=1,alpha=0.6)
        for bar,val in zip(bars,rms):
            ax1.text(bar.get_x()+bar.get_width()/2,bar.get_height()+0.3,
                     f'{val:.1f}',ha='center',fontsize=7,color='#c0d4e8')
        ax1.set_xticks(x); ax1.set_xticklabels(names,fontsize=7,rotation=10)
        ax1.set_ylabel('RMS (mm)',fontsize=8); ax1.set_title('LC/LW RMS Residual',fontsize=9,color='#c0d4e8')
        for sp in ax1.spines.values(): sp.set_edgecolor('#1e3050')

        # Cycle slip count
        ax2=fig.add_subplot(gs[1])
        bars2=ax2.bar(x,slip,color=cols,alpha=0.75,edgecolor='#0d1b2e',linewidth=0.5)
        for bar,val in zip(bars2,slip):
            ax2.text(bar.get_x()+bar.get_width()/2,bar.get_height()+0.1,
                     str(val),ha='center',fontsize=7,color='#c0d4e8')
        ax2.set_xticks(x); ax2.set_xticklabels(names,fontsize=7,rotation=10)
        ax2.set_ylabel('Detected Slips',fontsize=8); ax2.set_title('Cycle Slip Count',fontsize=9,color='#c0d4e8')
        for sp in ax2.spines.values(): sp.set_edgecolor('#1e3050')

        fig.suptitle('Linear Combination Analysis',fontsize=10,color='#c0d4e8',y=1.01)
    buf=io.BytesIO(); fig.savefig(buf,format='png',dpi=150,bbox_inches='tight',facecolor=fig.get_facecolor()); plt.close(fig); buf.seek(0); return buf

# ── Main ──────────────────────────────────────────────────────────────────────
def generate_report(job: dict, output_path: str) -> str:
    from lc_lw_analysis import (
        parse_pos_full, gen_demo_solution, solution_to_summary_dict, FullSolution
    )

    summary = job.get('summary') or {}
    s = _styles()
    W,H = A4
    margin = 20*mm
    cw = W - 2*margin

    doc = SimpleDocTemplate(output_path, pagesize=A4,
        leftMargin=margin, rightMargin=margin,
        topMargin=22*mm, bottomMargin=16*mm,
        title='GNSS Processing Report', author='KJSB Randy dan Rekan')

    on_page = lambda canvas,doc: _on_page(canvas,doc,job)

    # Load full solution
    pos_file = job.get('result_file')
    if pos_file and os.path.exists(pos_file):
        sol = parse_pos_full(pos_file) or gen_demo_solution(job.get('mode','static'))
    else:
        sol = gen_demo_solution(job.get('mode','static'))

    # Merge sol stats into summary for compatibility
    sol_dict = solution_to_summary_dict(sol)
    for k,v in sol_dict.items():
        if k not in summary or not summary[k]:
            summary[k] = v

    # Generate all plots
    buf_ts   = _plot_timeseries(sol.epochs, summary)
    buf_sc   = _plot_scatter(sol.epochs, summary)
    buf_map  = _plot_map(summary.get('lat_mean',-7.0), summary.get('lon_mean',112.7))
    buf_sq   = _plot_solution_quality(sol)
    buf_lc   = _plot_lc_bars(summary.get('lc_info',[]))

    story = []

    # ══ PAGE 1 ════════════════════════════════════════════════════════════════
    story.append(Spacer(1,8*mm))
    story.append(Paragraph('GNSS POST-PROCESSING',s['GT']))
    story.append(Paragraph('FIELD SURVEY REPORT — v2',s['GS']))
    story.append(ThinRule(cw,C_ACCENT,1.5)); story.append(Spacer(1,5*mm))

    # Processing info
    created=job.get('created_at','')[:19].replace('T',' ')
    ml={'single':'Single Point','dgps':'DGPS/DGNSS','kinematic':'Kinematic',
        'static':'Static','ppp-static':'PPP-Static','ppp-kinematic':'PPP-Kinematic'}
    is_demo=summary.get('demo',False)
    params=job.get('params',{})

    meta=[
        ['Parameter','Value'],
        ['Job ID',f"#{job.get('id','—')}"],
        ['Processing Mode',ml.get(job.get('mode','static'),'Static')],
        ['Rover File',job.get('rover','—')],
        ['Base File',job.get('base') or '— (PPP/Single)'],
        ['Navigation File',job.get('nav','—')],
        ['Frekuensi',{'1':'L1','2':'L1+L2','3':'L1+L2+L5','4':'L1+L2+L5+L6'}.get(params.get('freq','2'),'L1+L2')],
        ['Ionosfer Model',['OFF','Klobuchar','SBAS','Dual-freq IF','Est TEC','IONEX','QZSS'].index.__class__.__name__ and
            ['OFF','Klobuchar','SBAS','Dual-freq IF','Est TEC','IONEX','QZSS'][int(params.get('iono',3))]],
        ['Troposfer Model',['OFF','Saastamoinen','SBAS','Est ZTD'][int(params.get('tropo',1))]],
        ['Ambiguity Resolution',params.get('amb_res','continuous').title()],
        ['Elevation Mask',f"{params.get('elevation_mask','15')}°"],
        ['Processed At',created],
        ['Data Status','⚠ DEMO DATA' if is_demo else '✓ Real RINEX processed'],
    ]
    mt=Table(meta,colWidths=[55*mm,cw-55*mm])
    mt.setStyle(TableStyle(_ts([('TEXTCOLOR',(-1,-1),(-1,-1),C_WARN if is_demo else C_GOOD)])))
    story.append(KeepTogether([SectionHeader('01','Processing Information',cw),Spacer(1,3),mt,Spacer(1,5*mm)]))

    # Coordinates
    lat=summary.get('lat_mean',0); lon=summary.get('lon_mean',0); ht=summary.get('height_mean',0)
    ls=summary.get('lat_std',0); lns=summary.get('lon_std',0); hts=summary.get('ht_std',0)
    cd=[
        ['Component','Mean Value','Std Dev (m)','Precision Class'],
        ['Latitude (°)',f'{lat:.8f}',f'±{ls:.5f}',_prec(ls)],
        ['Longitude (°)',f'{lon:.8f}',f'±{lns:.5f}',_prec(lns)],
        ['Height (m)',f'{ht:.4f}',f'±{hts:.5f}',_prec(hts)],
    ]
    cts=_ts()
    for i,(v,col) in enumerate([(ls,C_GOOD if ls<0.005 else C_WARN),(lns,C_GOOD if lns<0.005 else C_WARN),(hts,C_GOOD if hts<0.010 else C_WARN)],start=1):
        cts+=[('TEXTCOLOR',(3,i),(3,i),C_GOOD if v<0.005 else C_WARN if v<0.020 else C_BAD),
              ('FONTNAME',(3,i),(3,i),'Courier-Bold')]
    ct=Table(cd,colWidths=[45*mm,60*mm,35*mm,cw-140*mm]); ct.setStyle(TableStyle(cts))
    story.append(KeepTogether([SectionHeader('02','Coordinate Results',cw),Spacer(1,3),ct,Spacer(1,5*mm)]))

    # Quality stats
    total=summary.get('total_epochs',0); fixed=summary.get('fixed_epochs',0)
    ratio=summary.get('fix_ratio',0)
    cep=summary.get('cep_mm',0); sep=summary.get('sep_mm',0)
    rms_h=summary.get('rms_h',0); rms_v=summary.get('rms_v',0); rms_3d=summary.get('rms_3d',0)
    sd=[
        ['Metric','Value','Metric','Value'],
        ['Total Epochs',f'{total:,}','CEP (2D)',f'{cep:.2f} mm'],
        ['Fixed (Q=1)',f'{fixed:,}','SEP (3D)',f'{sep:.2f} mm'],
        ['Float (Q=2)',f'{summary.get("float_epochs",0):,}','RMS Horizontal',f'{rms_h:.2f} mm'],
        ['Fix Ratio',f'{ratio:.1f}%','RMS Vertical',f'{rms_v:.2f} mm'],
        ['Avg Satellites',f'{summary.get("ns_mean",0):.1f}','RMS 3D',f'{rms_3d:.2f} mm'],
        ['Amb AR Ratio',f'{summary.get("amb_ratio",0):.1f}%','PDOP Mean',f'{summary.get("pdop_mean",0):.2f}'],
    ]
    sts=_ts()
    rc=C_GOOD if ratio>=80 else C_WARN if ratio>=50 else C_BAD
    sts+=[('TEXTCOLOR',(1,4),(1,4),rc),('FONTNAME',(1,4),(1,4),'Courier-Bold')]
    st=Table(sd,colWidths=[55*mm,35*mm,55*mm,cw-145*mm]); st.setStyle(TableStyle(sts))
    story.append(KeepTogether([SectionHeader('03','Quality Statistics',cw),Spacer(1,3),st,Spacer(1,5*mm)]))

    # Map
    img_map=Image(buf_map,width=cw*0.48,height=cw*0.38)
    info_lines=[f'Lat : {lat:.8f} °',f'Lon : {lon:.8f} °',f'Ht  : {ht:.4f} m','',
                f'SDn : {summary.get("sdn_mean",0)*1000:.2f} mm',
                f'SDe : {summary.get("sde_mean",0)*1000:.2f} mm',
                f'SDu : {summary.get("sdu_mean",0)*1000:.2f} mm','',
                'Datum: WGS-84 Ellipsoidal','Ref Frame: ITRF2020 (est.)']
    info_p=Paragraph('<br/>'.join(info_lines),
                     ParagraphStyle('IP',fontName='Courier',fontSize=8,textColor=C_LIGHT,leading=13))
    mt2=Table([[img_map,Spacer(1,1),info_p]],colWidths=[cw*0.5,4*mm,cw*0.46])
    mt2.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'MIDDLE'),('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(-1,-1),0)]))
    story.append(KeepTogether([SectionHeader('04','Location Map',cw),Spacer(1,3),mt2,
        Paragraph('Figure 1. Estimated point location. Radius ±500m.',s['GC']),Spacer(1,4*mm)]))

    # ══ PAGE 2 ════════════════════════════════════════════════════════════════
    story.append(PageBreak()); story.append(Spacer(1,4*mm))

    img_ts=Image(buf_ts,width=cw,height=cw*0.60)
    story.append(KeepTogether([SectionHeader('05','Position Time Series',cw,C_ACC2),Spacer(1,3),img_ts,
        Paragraph('Figure 2. Per-epoch deviation from mean. Green=Fixed(Q=1), Yellow=Float(Q=2), Red=Single. Dashed=±3σ.',s['GC']),Spacer(1,5*mm)]))

    img_sc=Image(buf_sc,width=cw,height=cw*0.43)
    story.append(KeepTogether([SectionHeader('06','Precision Analysis',cw,C_ACC2),Spacer(1,3),img_sc,
        Paragraph('Figure 3. Left: N/E scatter + CEP circle. Right: Height residual histogram.',s['GC']),Spacer(1,5*mm)]))

    # ══ PAGE 3 — Solution Quality ═════════════════════════════════════════════
    story.append(PageBreak()); story.append(Spacer(1,4*mm))

    img_sq=Image(buf_sq,width=cw,height=cw*0.50)
    story.append(KeepTogether([SectionHeader('07','Solution Quality Analysis',cw,C_ACC2),Spacer(1,3),img_sq,
        Paragraph('Figure 4. Left: Q-distribution pie. Centre: satellite count. Right: solution type per epoch.',s['GC']),Spacer(1,5*mm)]))

    # Detailed solution table
    sol_tbl_data=[
        ['Metric','Value','Metric','Value'],
        ['Fixed Epochs (Q=1)',f'{sol.n_fixed:,}','Float Epochs (Q=2)',f'{sol.n_float:,}'],
        ['Single Epochs (Q=5)',f'{sol.n_single:,}','DGPS Epochs (Q=4)',f'{sol.n_dgps:,}'],
        ['SBAS Epochs (Q=3)',f'{sol.n_sbas:,}','PPP Epochs (Q=6)',f'{sol.n_ppp:,}'],
        ['Fix Ratio',f'{sol.fix_ratio:.2f}%','Amb AR Ratio',f'{sol.amb_ratio:.1f}%'],
        ['Ambiguity Fix Count',f'{sol.amb_fix_count:,}','Amb Total',f'{sol.amb_total:,}'],
        ['Satellite Mean',f'{sol.ns_mean:.1f}','Satellite Min/Max',f'{sol.ns_min}/{sol.ns_max}'],
        ['PDOP Mean (est.)',f'{sol.pdop_mean:.2f}','PDOP Max (est.)',f'{sol.pdop_max:.2f}'],
        ['SDn Mean',f'{sol.sdn_mean*1000:.2f} mm','SDe Mean',f'{sol.sde_mean*1000:.2f} mm'],
        ['SDu Mean',f'{sol.sdu_mean*1000:.2f} mm','RMS 3D',f'{sol.rms_3d*1000:.2f} mm'],
    ]
    sol_tbl=Table(sol_tbl_data,colWidths=[55*mm,35*mm,55*mm,cw-145*mm])
    sol_tbl.setStyle(TableStyle(_ts()))
    story.append(KeepTogether([SectionHeader('08','Detailed Solution Table',cw),Spacer(1,3),sol_tbl,Spacer(1,5*mm)]))

    # ══ PAGE 4 — LC/LW ════════════════════════════════════════════════════════
    story.append(PageBreak()); story.append(Spacer(1,4*mm))

    lc_info=summary.get('lc_info',[])
    if lc_info:
        img_lc=Image(buf_lc,width=cw,height=cw*0.38)
        story.append(KeepTogether([SectionHeader('09','Linear Combination Analysis',cw,C_ACC2),Spacer(1,3),img_lc,
            Paragraph('Figure 5. LC/LW RMS residual (mm) dan jumlah cycle slip terdeteksi per kombinasi linear.',s['GC']),Spacer(1,5*mm)]))

        # LC table
        lc_tbl_data=[['Kombinasi','λ (cm)','Deskripsi','RMS (mm)','Std (mm)','Slip']]
        for lc in lc_info:
            lw=f'{lc["wavelength"]:.1f}' if lc['wavelength']>0 else 'N/A'
            lc_tbl_data.append([
                lc['name'], lw, lc['description'][:30]+'…' if len(lc['description'])>30 else lc['description'],
                f'{lc["rms_mm"]:.2f}', f'{lc["std_mm"]:.2f}', str(lc['n_slip']),
            ])
        lc_tbl=Table(lc_tbl_data,colWidths=[38*mm,16*mm,65*mm,20*mm,20*mm,12*mm])
        lc_tbl.setStyle(TableStyle(_ts()))
        story.append(KeepTogether([SectionHeader('10','LC/LW Summary Table',cw),Spacer(1,3),lc_tbl,Spacer(1,4*mm)]))

        # LC descriptions
        story.append(SectionHeader('11','Penjelasan Kombinasi Linear',cw)); story.append(Spacer(1,3))
        for lc in lc_info:
            story.append(Paragraph(f'<b>{lc["name"]}</b>  —  {lc["description"]}',
                                   ParagraphStyle('LCH',fontName='Courier-Bold',fontSize=8,textColor=C_ACCENT,leading=12,spaceAfter=2)))
            story.append(Paragraph(lc['use'],
                                   ParagraphStyle('LCD',fontName='Courier',fontSize=7.5,textColor=C_LIGHT,leading=11,spaceAfter=6,leftIndent=10)))


    # ══ CRS OUTPUT PAGE ═══════════════════════════════════════════════════════
    story.append(PageBreak()); story.append(Spacer(1,4*mm))

    # Load CRS conversions
    try:
        from crs_transform import convert_all, results_to_dict
        crs_results = convert_all(
            summary.get('lat_mean', 0),
            summary.get('lon_mean', 0),
            summary.get('height_mean', 0)
        )
        crs_list = results_to_dict(crs_results)
    except Exception as e:
        crs_list = []

    story.append(SectionHeader('CRS', 'Coordinate Reference System Output', cw, C_ACC2))
    story.append(Spacer(1, 4))

    if crs_list:
        # Priority CRS first
        PRIORITY = [4326, 9470, 32748, 32749, 32750, 23829, 23830, 23831, 4978]
        sorted_crs = sorted(crs_list,
            key=lambda c: PRIORITY.index(c['epsg']) if c['epsg'] in PRIORITY else 99)

        for crs in sorted_crs:
            keys  = list(crs['coords'].keys())
            vals  = list(crs['coords'].values())
            epsg_str = f"EPSG:{crs['epsg']}" if crs['epsg'] else 'Custom'
            is_key = crs['epsg'] in [32749, 23830, 4326, 9470]

            # Header row
            hdr_col = C_ACCENT if is_key else C_ACC2
            row_data = [[epsg_str, crs['name'], crs['unit']]]
            for k, v in zip(keys, vals):
                fmt = f'{v:.8f}' if abs(float(v)) < 200 and crs['unit']=='degree'                       else f'{v:.3f}' if crs['unit']=='metre'                       else str(v)
                row_data.append([k, fmt, ''])

            if crs.get('note'):
                row_data.append(['Note', crs['note'], ''])

            crs_tbl = Table(row_data, colWidths=[38*mm, 95*mm, 22*mm])
            ts_crs = [
                ('FONTNAME',  (0,0),(-1,-1),'Courier'),
                ('FONTSIZE',  (0,0),(-1,-1),8),
                ('FONTNAME',  (0,0),(-1,0), 'Courier-Bold'),
                ('BACKGROUND',(0,0),(-1,0), C_PANEL if is_key else HexColor('#0f1e2e')),
                ('TEXTCOLOR', (0,0),(0,0),  hdr_col),
                ('TEXTCOLOR', (1,0),(1,0),  C_LIGHT),
                ('TEXTCOLOR', (2,0),(2,0),  C_DIM),
                ('ROWBACKGROUNDS',(0,1),(-1,-1),[C_WHITE, C_TBLALT]),
                ('TEXTCOLOR', (0,1),(-1,-1),C_DARK),
                ('FONTNAME',  (0,1),(0,-1), 'Courier-Bold'),
                ('TEXTCOLOR', (0,1),(0,-1), HexColor('#334466')),
                ('GRID',      (0,0),(-1,-1),0.3,C_DIM),
                ('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),
                ('TOPPADDING', (0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4),
                ('VALIGN',    (0,0),(-1,-1),'MIDDLE'),
            ]
            if crs.get('note'):
                ts_crs += [
                    ('SPAN',       (1,-1),(2,-1)),
                    ('FONTSIZE',   (0,-1),(-1,-1),7),
                    ('TEXTCOLOR',  (0,-1),(-1,-1),C_DIM),
                    ('BACKGROUND', (0,-1),(-1,-1),HexColor('#f8faff')),
                ]
            crs_tbl.setStyle(TableStyle(ts_crs))
            story.append(KeepTogether([crs_tbl, Spacer(1, 4)]))

    else:
        story.append(Paragraph('CRS conversion data tidak tersedia.', s['GN']))

    story.append(Spacer(1, 4*mm))

    # Recommendations
    fix_interp='Fix ratio >80% — ambiguity resolution baik.' if ratio>=80 else 'Fix ratio <80% — periksa baseline, multipath, atau durasi observasi.'
    prec_interp=f'CEP 2D = {cep:.2f} mm. {"Memenuhi" if cep<10 else "Melebihi"} toleransi survei kadaster (≤10mm).'
    rec=[
        ['Aspek','Nilai','Interpretasi'],
        ['Fix Ratio',f'{ratio:.1f}%',fix_interp],
        ['CEP 2D',f'{cep:.2f} mm',prec_interp],
        ['Tinggi',f'{hts*1000:.2f} mm','Baik (<15mm)' if hts*1000<15 else 'Perlu verifikasi (>15mm)'],
        ['Ambiguity AR',f'{summary.get("amb_ratio",0):.1f}%','Baik (>80%)' if summary.get("amb_ratio",0)>=80 else 'Perlu evaluasi'],
        ['Rekomendasi','—','Verifikasi dengan ≥2 sesi independen & bandingkan dengan titik BIG/CORS terdekat.'],
    ]
    rt=Table(rec,colWidths=[35*mm,28*mm,cw-63*mm])
    rt.setStyle(TableStyle(_ts([('TEXTCOLOR',(0,1),(0,-1),C_ACC2),('FONTNAME',(0,1),(0,-1),'Courier-Bold')])))
    story.append(Spacer(1,4*mm))
    story.append(KeepTogether([SectionHeader('12','Interpretasi & Rekomendasi',cw),Spacer(1,3),rt]))

    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
    return output_path
