\c 20 200
.qipc.conns:1#([hp:`$()] h:"i"$(); isOpen:"b"$(); attempts:"j"$(); opts:(); onOpen:(); onClose:()) 

// ====================== Logging
.qipc.log.msg:{[l;f;m;o] 
  -1 "[",string[.z.p],"][",string[.z.h],"][",l,"][",string[.z.i],"][",string[f],"]: ",m," -- ",$[o~();"";.Q.s1 o];
  };
.qipc.log.info: .qipc.log.msg[" INFO";`qipc.q];
.qipc.log.debug:.qipc.log.msg["DEBUG";`qipc.q];
.qipc.log.error:.qipc.log.msg["ERROR";`qipc.q];
.qipc.log.warn: .qipc.log.msg[" WARN";`qipc.q];
// ======================

// ====================== Timer
.qipc.timer.timer:1#([id:"j"$()] nextRun:"p"$(); repeatFreq:"n"$(); command:());

.qipc.timer.add:{[st;rep;fp;overwrite]
  .qipc.log.info["Adding timer";`startTime`repeatFrequncy`command!(st;rep;fp)]
  if[overwrite; .qipc.timer.remove fp];
  id:{$[0W=abs x;1;1+x]}exec max id from .qipc.timer.timer;
  `.qipc.timer.timer upsert (id;st;rep;fp);
  };
.qipc.timer.remove:{[fp] delete from `.qipc.timer.timer where command~\:fp};;

.qipc.timer.check:{[]
  toRun:0!select from .qipc.timer.timer where nextRun <=.z.p,not null nextRun;
  if[not count toRun; :()];
  {[x]
    @[value;x`command;{[cmd;x] .qipc.log.error["Error running timer command";`command`error!(cmd;x)]}x`command];
    if[not null x`repeatFreq;
      nextRun:.z.p + x`repeatFreq;
      .qipc.timer.timer[x`id;`nextRun]:nextRun
      ];
    } each toRun;
  };

.z.ts:{.qipc.timer.check[]};
\t 100
//=======================

// ====================== UTIL
.qipc.obfs:{$[4=count ":"vs string x;":"sv 2#s;string x]}
// ======================

// ====================== CORE
.qipc.init:{[hp;opts;onOpen;onClose]
  .qipc.log.info["Initialising connection to ",.qipc.obfs hp;`opts`onOpen`onClose!(opts;onOpen;onClose)];
  `.qipc.conns upsert `hp`h`isOpen`attempts`opts`onOpen`onClose!(hp;0N;0b;0;opts;onOpen;onClose);
  .qipc.open[hp];
  };

.qipc.open:{[hp]
  .qipc.timer.remove(`.qipc.open;hp);
  c:.qipc.conns hp;
  if[c`isOpen;:()];
  obfshp:.qipc.obfs hp;
  .qipc.log.info["Opening handle to ",obfshp;()];
  h:@[hopen;hp;{[hp;x] .qipc.log.error["Error connecting to ",hp;x]; -1}obfshp];
  if[h<0;
    .qipc.conns[hp;`attempts]+:1;
    attempts:.qipc.conns[hp;`attempts];
    .qipc.log.info["Attempt ",string[attempts]," failed";hp];
    if[maBreach:(ma:c[`opts][`maxAttempts]) <= attempts;
      .qipc.log.warn["Max attempts (",string[ma],") has been reached for ",obfshp;()];
      if[c[`opts][`die];
        .qipc.log.info[obfshp,".opts.die = true. Exiting with error code 1";()];
        exit 1;
        ];  
      ];
    if[not[maBreach] and not null rp:c[`opts][`retryPeriod];
      .qipc.timer.add[.z.p + rp * 0D00:00:00.001;0Nn;(`.qipc.open;hp);1b];
      ];
    :();
    ];
  .qipc.log.info["Connection successful. Handle is";h];  
  .qipc.conns[hp;`h`isOpen`attempts]:(h;1b;0);

  .qipc.onOpen[update hp:hp from .qipc.conns hp]
  };

.qipc.onClose:{[c]
  obfshp:.qipc.obfs c`hp;
  .qipc.log.error["Lost connection to ",obfshp," (",string[c`h],")";()];
  .qipc.conns[c`hp;`h`isOpen]:(0N;0b);
  if[c[`onClose][`die];
    .qipc.log.info[obfshp,".onClose.die = true. Exiting with error code 1";()];
    exit 1
    ];
  if[c[`onClose][`retry];
    .qipc.log.info[obfshp,".onClose.retry = true";()];
    .qipc.open c`hp
    ];
  };

.qipc.onOpen:{[c]
  obfshp:.qipc.obfs c`hp;
  P:`h`hp#c;
  rc:c[`onOpen][`remote];
  lc:c[`onOpen][`local];
  if[not null rc`func;
    .qipc.log.info["Found function to run in ",obfshp,".onOpen.remote.func";rc];
    p:P,rc`params;
    ($[rc`async;neg c`h;c`h])(value;(rc`func;p));
    ];
  if[not null lc`func;
    .qipc.log.info["Found function to run in ",obfshp,".onOpen.local.func";lc];
    p:P,lc`params;
    @[value;(lc`func;p);{.qipc.log.error["Error running onOpen.loca.func";x]}];
    ];
  };

.z.pc:{[x]
  c:first select from 0!.qipc.conns where h=x;
  if[null c`h; :()];
  .qipc.onClose c
  };
// ======================


\
.qipc.init[`::8055;`maxAttempts`retryPeriod`die!(3;10000;1b);`local`remote!(`func`params!({show `calledLocalFunc};()!());`func`params`async!({show x};`a`b!1 2;0b));`die`retry!01b]



