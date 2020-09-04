/ \d .news
// system"p 5001"
\c 100 300
k)quantileK:{avg x(<x)@_y*-1 0+#x,:()};
quantile:{[x;N](asc x)floor N*count x};
ungroup1: {[col;tbl] @[tbl where count each tbl col;col;:;raze tbl col]};
findK:{{first raze x}each ss\:/:[lower[x];$[10h~type y;enlist y;y]]}; // x is groupped prevHeadline column, y list of kwords
likeK:{not $[`any~z;all;any] 0N=findK[x;lower[y]]}; // returns 1b if finds all kwords in any of prevHeadlines
followK:{all 0<1_deltas findK[x;reverse lower[y]]}; // returns 1b if finds kwords in the same order
getNewsRet:{[news;rcut]
    / newsR:update date:"d"$first'[vs'["T";sv'[".";vs'["-";string GmtTimeStamp]]]],sym:`$symbols,id from ungroup select symbols,syms,id,gmtstamp,GmtTimeStamp,Headline from news;
    newsR:update date:"d"$GmtTimeStamp,sym:`$symbols,id from ungroup select symbols,syms,id,gmtstamp,GmtTimeStamp,Headline from news;
    newsR:newsR lj 2!(select from eodR where date in exec distinct date from newsR);
    newsRes:select from newsR where -10<>type'[ret],(rcut<abs(ret5-1))|(rcut<abs(ret-1));
    :0!select symbols,syms,`float$ret,ret5,ret20,first date,first gmtstamp,first Headline by id from newsRes;
    };
testNewsRet:{[news;rcut]
    :select from news;
    };
// Market On Move Analytics (aka reverse algorithm)
genPrevNews:{[newsRet;NN]
    genPrev:{[N;C]sv'[">";except\:[flip 1_{next x}\[N;C];enlist""]]};
    rr:`sym xgroup `date xdesc newsRet;
    // select if number of non-empty Headlines for different dates is greater equal to NN
    if[not `~NN;rr:select from rr where NN<=sum'[each'[{not x like ""};Headline]]];
    rr:update prevHeadline:genPrev'[NN+1;Headline],prevDate:genPrev'[NN+1;string date],prevID:genPrev'[NN+1;newsID] from rr;
    if[`tags in cols rr;
        rr:update prevTags:genPrev'[NN+1;tags] from rr];
    rr:ungroup rr;
    // remove duplicate rows
    rr1:select from rr where 1=(count;i) fby sym,not newsID like "";
    rr2:select from rr where 1<(count;i) fby sym,(not prevID like "")|(not newsID like "");
    rr2:select from rr2 where i=(max;i) fby ([]prevID;newsID);
    rr:rr1,rr2;
    // rr:update GmtTimeStamp:{ssr[x;y[0];y[1]]}/[;("DT";".-";("00-00";"00.00"))]'[string "p"$date] from rr;
    rr:update GmtTimeStamp:string "p"$date from rr;
    :rr;
    };
showSimilarSVO:{[tt] // show all news with at least one common SVO or empty SVO
    kwords1:,/[exec vs'[",";SVO] from tt where not first'[vs'[",";SVO]] like ""];
    dkwd:distinct (asc kwords1)where ~':[asc kwords1];
    :select from tt where (any'[in[vs'[",";SVO];dkwd]])|(SVO like "");
    };
showSimilarNews:{[tt] // show all news with at least one common SVO or empty SVO
    kwords1:,/[exec vs'[" ";Headline] from tt where not first'[vs'[" ";Headline]] like ""];
    dkwd:distinct (asc kwords1)where ~':[asc kwords1];
    :select from tt where (any'[in[vs'[" ";Headline];dkwd]])|(Headline like "");
    };
loadNewsHDB:{[Dates;kwords;argDict]
    Syms:$[`Syms in key argDict;argDict`Syms;`];
    ICodes:$[`ICodes in key argDict;argDict`ICodes;`];
    rMax:$[`rMax in key argDict;argDict`rMax;0.017];
    NN:$[`NN in key argDict;argDict`NN;2];
    QQ:$[`QQ in key argDict;argDict`QQ;`];
    XFilt:$[`XFilt in key argDict;argDict`XFilt;`];
    if[`~Dates;Dates:(.z.d-5;.z.d)];
    Date2:last Dates;
    Q1:0.0;Q2:0.999;
    if[not `~QQ;Q1:QQ[0];Q2:QQ[1]]; / set quantiles
    if[not `~Syms;if[-11h=type Syms;Syms:(),Syms]];
    eodYr:$[not `~Syms;
        update ret:ret-1,ret5:ret5-1,ret20:ret20-1 from select from eodR where date within (Date2-360;Date2),sym in Syms,not null ret;
        update ret:ret-1,ret5:ret5-1,ret20:ret20-1 from select from eodR where date within (Date2-360;Date2),not null ret];
    `eodRet set select from eodYr where date within Dates;
    qdict:{[Q;eodYr]:{[Q;x]x[`sym]!quantile'[abs[x[`ret]];Q]}[Q]exec sym,ret from select ret by sym from eodYr};
    if[not `~QQ;
        {[qdict1;qdict2;eodRet;x]update qnt1:qdict1[x],qnt2:qdict2[x] from eodRet where sym=x}[qdict[Q1;eodYr];qdict[Q2;eodYr];`eodRet]each key qdict[Q1;eodYr]];
    filt:enlist[(within;`date;Dates)];
    if[not `~Syms;filt,:enlist[(in;`sym;enlist[Syms])]];
    if[(not `~ICodes)&(not ()~ICodes);
        filt,:$[0h~type ICodes;
            [cumFilt::(|;(like;`IndustryCodes;enlist"*",ICodes[0],"*");(like;`IndustryCodes;enlist"*",ICodes[1],"*"));
            {cumFilt::(|;(like;`IndustryCodes;enlist"*",x,"*");cumFilt)}each 2_ICodes;cumFilt];
            enlist[(like;`IndustryCodes;enlist"*",ICodes,"*")]
            ]];
    if[(not `~kwords)&(not ()~kwords);
        filt,:$[0h~type kwords;
            [cumFilt::(|;(like;(lower;`Headline);enlist"*",lower[kwords[0]],"*");(like;(lower;`Headline);enlist"*",lower[kwords[1]],"*"));
            {cumFilt::(|;(like;(lower;`Headline);enlist"*",x,"*");cumFilt)}each 2_lower[kwords];cumFilt];
            // enlist[(|),{(like;(lower;`Headline);enlist["*"],x,"*")}each lower[kwords]];
            enlist[(like;(lower;`Headline);enlist"*",lower[kwords],"*")]
            ]];
    if[not `~XFilt;filt,:enlist[(~:;(like;`Headline;XFilt))]];
    newsRet:?[news;filt;0b;()];
    if[0=count newsRet;-1"no news data";:newsRet];
    newsRet:delete sym1 from update sym:sym1 from update sym1:?[sym=`QQQQ;`QQQ;sym] from newsRet;
    // Join on date&sym, when we get intraday news this should change to second&sym or millisecond&sym
    newsRet:`date`sym xgroup delete ret from update string newsID from newsRet;
    newsRet:@[0!newsRet;(cols newsRet) except `date`sym`ret`CalDate;{sv["|";distinct x]}'];
    newsRet:lj[newsRet;2!eodRet];
    newsRet:$[`qnt1 in cols newsRet;
        select from newsRet where (qnt1&rMax)<abs[ret],qnt2>abs[ret];
        select from newsRet where rMax<abs[ret] ];
    // find and add prev tags
    if[`isTags in key argDict;
        tPath:hsym `$getenv[`WAPP],"/newsdai/data/tags.csv";
        tTags:string exec tag from ("sfifff*";enlist",") 0:tPath;
        newsRet:update tags:sv'["|";tTags where each like/:\:[lower[Headline];(,\:[,/:["* ";lower[tTags]];"*"])]] from newsRet where 1<(count;i) fby sym
    ];
    newsRet:genPrevNews[newsRet;NN];
    delete eodRet from `.;
    newsRet
    };
// usage: loadNewsHDB[(2007.02.20;2007.04.10);"pandemic";`rMax`NN`Syms!(0.01;2;`)]

/ getSimilarNewsOnMktMove[(2007.05.10;2007.05.15);0.05;`;0b;"*PRESS RELEASE*"]
mktMove2News:{[kwords;Dates;argDict]
    if[`~argDict;
        rMax:$[`~kwords;0.05;0.017];
        NN:$[`~kwords;3;1];
        QQ:$[`~kwords;`;(0.6;0.999)];
        ICodes:$[`~kwords;("XDJI";"XFFX";"XNYA";"XSP5");`];
        Syms:`; XFilt:`;
        argDict:`Syms`rMax`NN`QQ`XFilt`ICodes!(Syms;rMax;NN;QQ;XFilt;ICodes)];
    rMax:$[`rMax in key argDict;argDict`rMax;0.05];
    Date2:last Dates;isSimilar:0b;
    if[-14h~type Dates;Dates:(Dates-1;Dates)];
    mktNews:loadNewsHDB[Dates;`;argDict];
    kwords:lower[kwords];
    if[0=count mktNews;-1"returning empty ()";:()];
    if[(not `~kwords)&(10h~type kwords);
        // find any news where any prevHeadlines per symbol contain any of the keywords
        mktNews:$[0<first ss[kwords;">"];
            [kwords:">" vs kwords;select from mktNews where ((followK[;kwords];lower[prevHeadline]) fby sym)|((lower[prevHeadline] like "*",kwords[0],"*")&(lower[Headline] like "*",kwords[1],"*"))];
            select from mktNews where ((likeK[;kwords;`];lower[prevHeadline]) fby sym)|(lower[Headline] like "*",kwords,"*")];
        ];
    if[isSimilar;mktNews:showSimilarNews[mktNews]];
    mktNews:update Rank:abs[ret]%xexp[Date2+1-date;1.5],close from mktNews;
    outCols:`prevHeadline`Headline`sym`GmtTimeStamp`date`ret`ret5`ret20`newsID`prevID`prevDate`close;
    if[`isTags in key argDict;
        outCols,:`prevTags`tags];
    :`sym xasc `GmtTimeStamp xdesc distinct ?[mktNews;();0b;outCols!outCols];
    };
// mktMove2News["CEO";2007.05.14;`]
// usage: mktMove2News[`;("D"$"2007-01-01";"D"$"2007-03-31");`]
news2MktMove:{[kwords;Dates;argDict]
    if[`~argDict;
        Syms:`;rMax:0.05;NN:2;QQ:`;XFilt:`;ICodes:`;
        argDict:`Syms`rMax`NN`QQ`XFilt`ICodes!(Syms;rMax;NN;QQ;XFilt;ICodes)];
    if[-14h~type Dates;Dates:(Dates;Dates+1)];
    newsTb:loadNewsHDB[Dates;kwords;argDict];
    :$[`qnt1 in cols newsTb;select from newsTb where (qnt1|rMax)<abs[ret],qnt2>abs[ret];select from newsTb where rMax<abs[ret]];
    };
mktNews2Tags:{[kwords;mktTb]
    tPath:hsym `$getenv[`WAPP],"/newsdai/data/tags.csv";
    tTags:string exec tag from ("sfifff*";enlist",") 0:tPath;
    mktTb:update prevTags:tTags where each like/:\:[lower[prevHeadline];(,\:[,/:["* ";lower[tTags]];"*"])] from mktTb;
    mktTb:update tags:tTags where each like/:\:[lower[Headline];(,\:[,/:["* ";lower[tTags]];"*"])] from mktTb;
    if[not `~kwords;
        kwords:$[10h~type kwords;$[0<first ss[kwords;">"];">" vs kwords;enlist kwords];kwords];
        mktTb:@[mktTb;`tags;{[kk;x]x,kk}[kwords]'] ];
    evts:select first'[date],first'[sym],first'[ret],first'[ret5],first'[ret20],first'[price],tags,sv'["|";prevHD],sv'["|";prevID],first'[HD],first'[newsID] from select date,ret,ret5,ret20,HD:Headline,newsID,prevHD:distinct prevHeadline,distinct prevID,price:close,distinct raze tags by sym from mktTb;
    tagTb:0!select ret:avg ret,stdev:dev ret,Headline:sv["|";prevHD],ret5:avg ret5,ret20:avg ret20 by tags from ungroup1[`tags;evts] where price>1.0;
    $[`~kwords;
        `ret xdesc tagTb;
        (select from tagTb where any each like/:\:[tags;kwords]),(`ret xdesc delete from tagTb where any each like/:\:[tags;kwords]) ]
    };
// recommend tags a given keyword
findTags:{[kwords;Dates]
    mktTb:mktMove2News[kwords;Dates;`];
    mktNews2Tags[kwords;mktTb]
    };
// usage: findEvents["vaccine";(2007.02.15;2007.03.15)]
// find events series for a given keyword
findEvents:{[kwords;Dates]
    eTbl:mktMove2News[kwords;Dates;`];
    select first'[date],first'[sym],first'[ret],first'[ret5],sv'["|";prevHD],sv'["|";prevID],first'[HD],first'[newsID],first'[price] from select date,ret,ret5,HD:Headline,newsID,prevHD:distinct prevHeadline,distinct prevID,price:close by sym from eTbl where lower[prevHeadline] like ("*",lower[kwords],"*")
    };
findLoop:{[kwords;NN;Date1]
    res:();if[`~NN;NN:12];
    ii:0;do[NN;Dates:(Date1+30*ii;Date1+30*(ii+1));r1:.[findEvents;(kwords;Dates);{x}];if[(0<count r1)&(98h~type r1);show r1;res,:r1];ii+:1];
    res
    };
analyzeTags:{[kwords;Dates]
    mktTb::mktMove2News[kwords;Dates;`];
    mktNews2TagsCombo[kwords;mktTb]
    };
mktNews2TagsCombo:{[kwords;mktTb]
    if[`~kwords;mktTb:select from mktTb where close>1.0];
    mktTb:select from mktTb where not Headline like "",not prevHeadline like "";
    if[()~mktTb;-1"returning empty";:mktTb];
    tPath:hsym `$getenv[`WAPP],"/newsdai/data/tags.csv";
    tTags:lower string exec tag from ("sfifff*";enlist",") 0:tPath;
    mktTb:update lower prevHeadline,lower Headline from mktTb;
    if[not `~kwords;
        kwords:$[10h~type kwords;$[0<first ss[kwords;">"];">" vs kwords;enlist kwords];kwords];
        mktTb:$[1<count kwords;
            select from mktTb where prevHeadline like ("*",kwords[0],"*"),Headline like ("*",kwords[1],"*");
            select from mktTb where (prevHeadline like "*",kwords[0],"*")|(Headline like "*",kwords[0],"*")];
        tTags,:kwords];
    mktTb:update prevTags:tTags where each like/:\:[prevHeadline;(,\:[,/:["* ";tTags];"*"])] from mktTb;
    mktTb:update currTags:tTags where each like/:\:[Headline;(,\:[,/:["* ";tTags];"*"])] from mktTb;
    tagTb:update tags:{sv'["-";x]} each string each cross'[`$prevTags;`$currTags] from mktTb;
    tagTb:0!select ret:avg ret,stdev:dev ret,sv["|";prevHeadline],sv["|";Headline],sym,date,ret5:avg ret5,ret20:avg ret20 by tags from ungroup1[`tags;tagTb];
    :`ret xdesc tagTb;
    };
// for strat1
getMktMoves:{[dates;syms;mktCut]
    if[`~mktCut;mktCut:0.1]; // mkt impact threshold
    if[`~dates;dates:(2005.03.23;2008.03.30)];
    :$[`~syms;
        select from eodR where date within dates,mktCut<abs[1-ret],1.1<close,500000<volume*close;
        select from eodR where date within dates,sym in syms,mktCut<abs[1-ret],1.1<close,500000<volume*close];
    };
runStrat1:{[dates;mktCut;fact1;nDays;kwords]
    if[`~dates;dates:(2005.03.23;2008.03.30)];
    if[`~mktCut;mktCut:0.05]; // mkt impact threshold
    if[`~nDays;nDays:10]; // strategy time horizon in days
    if[`~fact1;fact1:3]; // multiplication factor for news threshold
    df2:select date,sym,ret from eodR where date within dates,mktCut<abs[1-ret],1.1<close,500000<volume*close;
    dSyms:distinct exec sym from df2;
    news1:select from news where date within dates,sym in dSyms;
    df0:$[`~kwords;select count'[Story],Kwords:0 by date,sym from news1;select count'[Story],Kwords:sum/[lower[Headline] like/:{"*",x,"*"} each kwords] by date,sym from news1];
    // if[`~kwords;df0:select from df0 where any Headline like/:{"*",x,"*"} each kwords];
    df1:select date,sym,Cnt:{sum distinct x} each Story,Kwords from df0;
    dt1:first (flip 0!df1)`date;
    dt2:last (flip 0!df1)`date;
    d1Syms:exec distinct sym from df1;
    df0:([]date:dt1+til[dt2+1-dt1]) cross ([] sym:d1Syms;Cnt:(count d1Syms)#0);
    df10:df0 lj 2!df1;
    df12:df10 lj 2!df2;
    nzlv:exec dev Cnt by sym from df12 where 0<Cnt,Cnt<(quantile[;0.75];Cnt) fby sym;
    nprev:{y til[count y]-\:1+til x};
    ////df22:ungroup select date,Cnt,ret,a1:?[((nzlv'[sym]*fact1)<maxCnt)&(Cnt>maxCnt)&(any'[0<,'[ret;nprev[nDays;ret]]]);?[any'[<[1;,'[ret;nprev[nDays;ret]]]];1;-1];0],Kwords by sym from ungroup select date,Cnt,ret,maxCnt:mmax[nDays;xprev[1;Cnt]],Kwords by sym from df12;
    //'break;
    //df22:ungroup select date,Cnt,ret,a1:?[((nzlv'[sym]*fact1)<Cnt)&(any'[0<,'[ret;nprev[nDays;ret]]]);?[any'[<[1;,'[ret;nprev[nDays;ret]]]];1;-1];0] by sym from ungroup select date,Cnt,ret,maxCnt:mmax[nDays;xprev[1;Cnt]] by sym from df12;
    df22:ungroup select date,Cnt,ret,a1:?[(0<ret);?[<[1;ret];1;-1];0] by sym from ungroup select date,Cnt,ret,maxCnt:mmax[nDays;xprev[1;Cnt]] by sym from df12;
    if[not `~kwords;df22:ungroup select sym,Kwords,Cnt,ret,date,a1:?'[1<&'[msum'[nDays;?'[a1<>0;1;0]];msum'[nDays;?'[Kwords<>0;1;0]]];1;0] from 0!(select a1,Kwords,Cnt,ret,date by sym from df22) where any'[Kwords>0]];
    df22:update fills close by sym from df22 lj 2!ungroup select close by sym,date from eodR where date within dates;
    pnl22:update pnl:?[0<>a1;1000*a1*(1-(xprev[neg[nDays];close]%next[close]));0] by sym from df22;
    //pnl22:update pnl:?[0>a1;1000*a1*((xprev[neg[nDays];close]%next[close])-1);0] by sym from df22;
    //finPnl:exec sum pnl from pnl22;
    finPnl:select tRet:%'[sum pnl;1000*max sums a1], cnt:count i, sharpe:%'[avg pnl;dev pnl] from pnl22 where a1<>0;
    res22:((dates;mktCut;fact1;kwords);pnl22;finPnl);
    (hsym `$"/tmp/strat1PNL-",string .z.z) set res22;
    res22
    };
// select ret:%'[sum pnl;1000*max sums a1], sharpe:%'[avg pnl;dev pnl] from res1 where a1<>0
/ \d .

