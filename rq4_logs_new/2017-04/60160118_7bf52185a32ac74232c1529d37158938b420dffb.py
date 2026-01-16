#python code that generates discreetMusic.sco
melody1 = [[ 0.0, 2.0, 72, 0.5],\
           [ 2.0, 2.5, 74, 0.5],\
           [24.0, 2.0, 64, 0.5],\
           [46.0, 2.0, 67, 0.5],\
           [76.0, 1.5, 76, 0.00],\
           [77.5, 1.5, 79, 0.33],\
           [79.0, 1.5, 81, 0.67],\
           [80.5, 3.5, 79, 1.00]]
melody2 = [[20.0, 2.0, 72, 0.5],\
           [24.0, 2.5, 74, 0.5],\
           [46.5, 2.0, 64, 0.5],\
           [48.5, 2.0, 67, 0.5],\
           [94.0, 1.5, 76, 0.5]]
for i in range(37):
  for j in range(22):
    for m in melody1:
      print 'i1 '+str(m[0]+12*j+i*136)+'\t'+str(m[1])+'\t'+str(m[2])+'\t'+str(0.85**j)+'\t'+str(m[3])
      print 'i1 '+str(m[0]+12*j+i*136+0.10)+'\t'+str(m[1])+'\t'+str(m[2]-0.2)+'\t'+str(0.85**j)+'\t'+str(m[3])
for i in range(34):
  for j in range(24):
    for m in melody2:
      print 'i1 '+str(m[0]+12*j+i*148)+'\t'+str(m[1])+'\t'+str(m[2])+'\t'+str(0.85**j)+'\t'+str(m[3])
      print 'i1 '+str(m[0]+12*j+i*148-0.1)+'\t'+str(m[1])+'\t'+str(m[2]+0.2)+'\t'+str(0.85**j)+'\t'+str(m[3])
