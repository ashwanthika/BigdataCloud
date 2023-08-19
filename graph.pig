var_g = LOAD '$G' USING PigStorage(',') AS (val_v:long, neighbour:long);
vertext_group = GROUP var_g BY val_v;
counter = FOREACH vertext_group GENERATE group as Ver, COUNT(var_g.neighbour) as neighb_count;
val_grpneig = GROUP counter BY neighb_count;
result = FOREACH val_grpneig GENERATE group, COUNT(counter.Ver);
STORE result INTO '$O' USING PigStorage ('\t');