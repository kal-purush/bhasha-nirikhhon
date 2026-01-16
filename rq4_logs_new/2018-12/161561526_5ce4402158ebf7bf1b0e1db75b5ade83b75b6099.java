import Cls.Decl.*;
import Cls.Stm.*;
import Cls.Type.*;
import Cls.Unit.*;
import Cls.Decl.Mod.*;
import java.util.*;

public class CSharpFile {
  private String strFile;
  public Unit parse_tree;

  public CSharpFile(String strFile) {
    this.strFile = strFile;
    parse_tree = null;
  }

   public CSharpFile() {
    this.strFile = "t1.txt";
    parse_tree = null;
  }

  public void Test() {
    Vector vecResult = new Vector();

    Vector vecDeclType = parse_tree.getVecDecl();
    int sizeVecDeclType = vecDeclType.size();
    for (int i=0; i<sizeVecDeclType; i++) {
      DeclType declType = (DeclType)vecDeclType.elementAt(i);
      if (declType instanceof DeclClass) {
        float f= COMLFMetric((DeclClass)declType);
        System.out.println("COMLFClass  - Class : " + declType.getName()  + " value : " + f);
        f= AnalyzabilityMetric((DeclClass)declType);
        System.out.println("Analy       - Class : " + declType.getName()  + " value : " + f);
        int k = UsabilityMetric((DeclClass)declType);
        System.out.println("Usable      - Class : " + declType.getName()  + " value : " + k);
        k = SpecialityMetric((DeclClass)declType);
        System.out.println("Special     - Class : " + declType.getName()  + " value : " + k);
        k = TestabilityMetric((DeclClass)declType);
        System.out.println("Testability - Class : " + declType.getName()  + " value : " + k);
        k = StabilityMetric((DeclClass)declType);
        System.out.println("Stability   - Class : " + declType.getName()  + " value : " + k);
      }

    }
  }

  public void Test1() {
    Vector vecResult = new Vector();

    Vector vecDeclType = parse_tree.getVecDecl();
    int sizeVecDeclType = vecDeclType.size();
    for (int i=0; i<sizeVecDeclType; i++) {
      DeclType declType = (DeclType)vecDeclType.elementAt(i);
      Vector vecDeclM = declType.getVecDecl();

      int sizeVecDeclM = vecDeclM.size();
      for (int j=0; j<sizeVecDeclM; j++) {
        Decl decl = (Decl)vecDeclM.elementAt(j);
        if ( (decl instanceof DeclConstructor)
          || (decl instanceof DeclDestructor)
          || (decl instanceof DeclMethod)
          || (decl instanceof DeclOperator)) {
          float f = COMLFMetric(decl);
          System.out.println("COMLFMetric - Method : " + decl.getName()  + " value : " + f);
          f = AnalyzabilityMetric(decl);
          System.out.println("AnalyMetric - Method : " + decl.getName()  + " value : " + f);
          int k =   LevlMetric(decl);
          System.out.println("Levl        - Method : " + decl.getName()  + " value : " + k);
          k =   TestabilityMetric(decl);
          System.out.println("Testability - Method : " + decl.getName()  + " value : " + k);
        }
      }
    }
  }

  public float AnalyzabilityMetric(Decl decl) {
    StmEmbedded body = decl.getStm();
    if (body instanceof StmEmpty)
      return 0;

    int ct_vg = getVg(body) + 1;

    int lc_stat = 0;
    Vector vecStm = body.getVecStm();
    if (vecStm != null)
      lc_stat = vecStm.size();

    float COMLF = COMLFMetric(decl);
    int ct_bran = getBranches(body);

    return ct_vg + lc_stat + COMLF + ct_bran;
  }

  // Numarul de decizii sau numarul ciclomatic
  private int getVg(Stm stm) {
    if ((stm == null) || (stm instanceof StmEmpty))
      return 0;

    int ct_vg = 0;
    if (stm instanceof StmSelection) {
      if (stm instanceof StmIf) {
        StmEmbedded stmT = ((StmIf)stm).getStmThen();
        ct_vg += getVg(stmT) + 1;
        StmEmbedded stmE = ((StmIf)stm).getStmElse();
        ct_vg += getVg(stmE);
      }

      if (stm instanceof StmSwitch) {
        Vector vecSec = ((StmSwitch)stm).getVecSec();
        if (vecSec != null) {
          int sizeVecSec = vecSec.size();
          for (int i=0; i<sizeVecSec; i++) {
            SwitchSection ss = (SwitchSection)vecSec.elementAt(i);
            ct_vg += ss.getVecLbl().size();
            Vector vecStm = ss.getVecStm();
            if (vecStm == null)
              continue;

            int sizeVecStm = vecStm.size();
            for (int j=0; j<sizeVecStm; j++) {
              Stm newStm = (Stm)vecStm.elementAt(j);
              ct_vg += getVg(newStm);
            }
          }
        }
      }

      return ct_vg;
    }

    if (stm instanceof StmBlock) {
      Vector vecStm = stm.getVecStm();
      if (vecStm != null) {
        int sizeVecStm = vecStm.size();
        for (int i=0; i<sizeVecStm; i++) {
          Stm newStm = (Stm)vecStm.elementAt(i);
          ct_vg += getVg(newStm);
        }
      }

      return ct_vg;
    }

    if ((stm instanceof StmChecked)
      || (stm instanceof StmTry)
      || (stm instanceof StmUnChecked)
      || (stm instanceof StmUnsafe)) {
      Stm newStm = ((StmEmbedded)stm).getBlock();
      ct_vg += getVg(newStm);

      if (stm instanceof StmTry) {
        FinallyClause fc = ((StmTry)stm).getFinallyClause();
        if (fc != null)
          ct_vg += getVg(fc.getBlock());

        CatchClause cc = ((StmTry)stm).getCatchClause();
        if (cc != null) {
          ct_vg += getVg(cc.getBlock());

          Vector vecSClause = cc.getVecSClause();
          if (vecSClause != null) {
            int sizeVecSClause = vecSClause.size();
            for (int i=0; i<sizeVecSClause; i++) {
              SpecificCatchClause scc = (SpecificCatchClause)vecSClause.elementAt(i);
              ct_vg += getVg(scc.getBlock());
            }
          }
        }
      }

      return ct_vg;
    }

   if ((stm instanceof StmFixed)
      || (stm instanceof StmIteration)
      || (stm instanceof StmLabeled)
      || (stm instanceof StmLock)
      || (stm instanceof StmUsing)) {
      StmEmbedded newStm = (StmEmbedded)stm.getStm();
      ct_vg += getVg(newStm);

      return ct_vg;
    }

    // is StmJump or StmThrow -> return 0;
    return ct_vg;
  }

  // Numarul de ramificari(de jump-uri)
  private int getBranches(Stm stm) {
    if ((stm == null) || (stm instanceof StmEmpty))
      return 0;

    int ct_bran = 0;
    if (stm instanceof StmSelection) {
      if (stm instanceof StmIf) {
        StmEmbedded stmT = ((StmIf)stm).getStmThen();
        ct_bran += getBranches(stmT);
        StmEmbedded stmE = ((StmIf)stm).getStmElse();
        ct_bran += getBranches(stmE);
      }

      if (stm instanceof StmSwitch) {
        Vector vecSec = ((StmSwitch)stm).getVecSec();
        if (vecSec != null) {
          int sizeVecSec = vecSec.size();
          for (int i=0; i<sizeVecSec; i++) {
            SwitchSection ss = (SwitchSection)vecSec.elementAt(i);
            Vector vecStm = ss.getVecStm();
            if (vecStm == null)
              continue;

            int sizeVecStm = vecStm.size();
            for (int j=0; j<sizeVecStm; j++) {
              Stm newStm = (Stm)vecStm.elementAt(j);
              ct_bran += getBranches(newStm);
            }
          }
        }
      }

      return ct_bran;
    }

    if (stm instanceof StmBlock) {
      Vector vecStm = stm.getVecStm();
      if (vecStm != null) {
        int sizeVecStm = vecStm.size();
        for (int i=0; i<sizeVecStm; i++) {
          Stm newStm = (Stm)vecStm.elementAt(i);
          ct_bran += getBranches(newStm);
        }
      }

      return ct_bran;
    }

    if ((stm instanceof StmChecked)
      || (stm instanceof StmTry)
      || (stm instanceof StmUnChecked)
      || (stm instanceof StmUnsafe)) {
      Stm newStm = ((StmEmbedded)stm).getBlock();
      ct_bran += getBranches(newStm);

      if (stm instanceof StmTry) {
        FinallyClause fc = ((StmTry)stm).getFinallyClause();
        if (fc != null)
          ct_bran += getBranches(fc.getBlock());

        CatchClause cc = ((StmTry)stm).getCatchClause();
        if (cc != null) {
          ct_bran += getBranches(cc.getBlock());

          Vector vecSClause = cc.getVecSClause();
          if (vecSClause != null) {
            int sizeVecSClause = vecSClause.size();
            for (int i=0; i<sizeVecSClause; i++) {
              SpecificCatchClause scc = (SpecificCatchClause)vecSClause.elementAt(i);
              ct_bran += getBranches(scc.getBlock());
            }
          }
        }
      }

      return ct_bran;
    }

   if ((stm instanceof StmFixed)
      || (stm instanceof StmIteration)
      || (stm instanceof StmLabeled)
      || (stm instanceof StmLock)
      || (stm instanceof StmUsing)) {
      StmEmbedded newStm = (StmEmbedded)stm.getStm();
      ct_bran += getBranches(newStm);

      return ct_bran;
    }

    if (stm instanceof StmJump)
      return ct_bran + 1;

    // is StmThrow -> return 0;
    return ct_bran;
  }

  public int TestabilityMetric(Decl decl) {
    StmEmbedded body = decl.getStm();
    if (body instanceof StmEmpty)
      return 0;

    int LEVL = LevlMetric(decl);
    int ct_path = getPaths(body);
    int ic_param = 0;
    if (!(decl instanceof DeclDestructor))
      ic_param = decl.getParameters();

    return LEVL + ct_path + ic_param;
  }

  private int getPaths(Stm stm) {
    if ((stm == null) || (stm instanceof StmEmpty))
      return 0;

    if (stm instanceof StmGoto)
      return -1;

    int ct_path = 0;
    if (stm instanceof StmSelection) {
      if (stm instanceof StmIf) {
        StmEmbedded stmT = ((StmIf)stm).getStmThen();
        int k = getPaths(stmT);
        if (k == -1)
          return -1;
        ct_path += k + 1;
        StmEmbedded stmE = ((StmIf)stm).getStmElse();
        if (stmE != null) {
          k = getPaths(stmE);
          if (k == -1)
            return -1;

          ct_path += k + 1;
        }
      }

      if (stm instanceof StmSwitch) {
        Vector vecSec = ((StmSwitch)stm).getVecSec();
        if (vecSec != null) {
          int sizeVecSec = vecSec.size();
          for (int i=0; i<sizeVecSec; i++) {
            SwitchSection ss = (SwitchSection)vecSec.elementAt(i);
            ct_path += ss.getVecLbl().size();
            Vector vecStm = ss.getVecStm();
            if (vecStm == null)
              continue;

            int sizeVecStm = vecStm.size();
            for (int j=0; j<sizeVecStm; j++) {
              Stm newStm = (Stm)vecStm.elementAt(j);
              int k = getPaths(newStm);
              if (k == -1)
                return -1;
              ct_path += k;
            }
          }
        }
      }

      return ct_path;
    }

    if (stm instanceof StmBlock) {
      Vector vecStm = stm.getVecStm();
      if (vecStm != null) {
        int sizeVecStm = vecStm.size();
        for (int i=0; i<sizeVecStm; i++) {
          Stm newStm = (Stm)vecStm.elementAt(i);
          int k = getPaths(newStm);
          if (k == -1)
            return -1;
          ct_path += k;
        }
      }

      return ct_path;
    }

    if ((stm instanceof StmChecked)
      || (stm instanceof StmTry)
      || (stm instanceof StmUnChecked)
      || (stm instanceof StmUnsafe)) {
      Stm newStm = ((StmEmbedded)stm).getBlock();
      int k = getPaths(newStm);
      if (k == -1)
        return -1;
      ct_path += k;

      if (stm instanceof StmTry) {
        FinallyClause fc = ((StmTry)stm).getFinallyClause();
        if (fc != null) {
          k = getPaths(fc.getBlock());
          if (k == -1)
            return -1;
          ct_path += k;
        }

        CatchClause cc = ((StmTry)stm).getCatchClause();
        if (cc != null) {
          k = getPaths(cc.getBlock());
          if (k == -1)
            return -1;
          ct_path += k;

          Vector vecSClause = cc.getVecSClause();
          if (vecSClause != null) {
            int sizeVecSClause = vecSClause.size();
            for (int i=0; i<sizeVecSClause; i++) {
              SpecificCatchClause scc = (SpecificCatchClause)vecSClause.elementAt(i);
              k = getPaths(scc.getBlock());
              if (k == -1)
                return -1;
              ct_path += k;
            }
          }
        }
      }

      return ct_path;
    }

   if ((stm instanceof StmFixed)
      || (stm instanceof StmIteration)
      || (stm instanceof StmLabeled)
      || (stm instanceof StmLock)
      || (stm instanceof StmUsing)) {
      StmEmbedded newStm = (StmEmbedded)stm.getStm();
      int k = getPaths(newStm);
      if (k == -1)
        return -1;
      ct_path += k;

      if (stm instanceof StmIteration)
        ct_path += 1;

      return ct_path;
    }

    // is StmBreak, StmContinue, StmReturn or StmThrow -> return 0;
    return ct_path;
  }

  public int LevlMetric(Decl decl) {
    StmEmbedded body = (StmEmbedded)decl.getStm();
    if (body instanceof StmEmpty)
      return 0;

    return Levl(body) + 1;
  }

  private int Levl(Stm stm) {
    if ((stm == null) || (stm instanceof StmEmpty))
      return 0;

    int nLevel = 0;
    if (stm instanceof StmSelection) {
      if (stm instanceof StmIf) {
        StmEmbedded stmT = ((StmIf)stm).getStmThen();
        int k1 = Levl(stmT);
        StmEmbedded stmE = ((StmIf)stm).getStmElse();
        int k2 = Levl(stmE);
        if (k1 > k2)
          nLevel += k1;
        else
          nLevel += k2;
      }

      if (stm instanceof StmSwitch) {
        Vector vecSec = ((StmSwitch)stm).getVecSec();
        if (vecSec != null) {
          int sizeVecSec = vecSec.size();
          for (int i=0; i<sizeVecSec; i++) {
            SwitchSection ss = (SwitchSection)vecSec.elementAt(i);
            Vector vecStm = ss.getVecStm();
            if (vecStm == null)
              continue;

            int sizeVecStm = vecStm.size();
            for (int j=0; j<sizeVecStm; j++) {
              Stm newStm = (Stm)vecStm.elementAt(j);
              int k1 = Levl(newStm);
              if (k1 > nLevel)
                nLevel = k1;
            }
          }
        }
      }

      return nLevel + 1;
    }

    if (stm instanceof StmBlock) {
      Vector vecStm = stm.getVecStm();
      if (vecStm != null) {
        int sizeVecStm = vecStm.size();
        for (int i=0; i<sizeVecStm; i++) {
          Stm newStm = (Stm)vecStm.elementAt(i);
          int k1 = Levl(newStm);
          if (k1 > nLevel)
            nLevel = k1;
        }
      }

      return nLevel;
    }

    if ((stm instanceof StmChecked)
      || (stm instanceof StmTry)
      || (stm instanceof StmUnChecked)
      || (stm instanceof StmUnsafe)) {
      Stm newStm = ((StmEmbedded)stm).getBlock();
      nLevel = Levl(newStm);

      if (stm instanceof StmTry) {
        int k;
        FinallyClause fc = ((StmTry)stm).getFinallyClause();
        if (fc != null) {
          k = Levl(fc.getBlock());
          if (k > nLevel)
            nLevel = k;
        }

        CatchClause cc = ((StmTry)stm).getCatchClause();
        if (cc != null) {
          k = Levl(cc.getBlock());
          if (k > nLevel)
            nLevel = k;

          Vector vecSClause = cc.getVecSClause();
          if (vecSClause != null) {
            int sizeVecSClause = vecSClause.size();
            for (int i=0; i<sizeVecSClause; i++) {
              SpecificCatchClause scc = (SpecificCatchClause)vecSClause.elementAt(i);
              k = Levl(scc.getBlock());
              if (k > nLevel)
                nLevel = k;
            }
          }
        }
      }

      return nLevel;
    }

   if ((stm instanceof StmFixed)
      || (stm instanceof StmIteration)
      || (stm instanceof StmLabeled)
      || (stm instanceof StmLock)
      || (stm instanceof StmUsing)) {
      StmEmbedded newStm = (StmEmbedded)stm.getStm();
      nLevel = Levl(newStm);

      if (stm instanceof StmIteration)
        nLevel += 1;

      return nLevel;
    }

    // is StmJump or StmThrow -> return 0;
    return nLevel;
  }

  public float COMLFMetric(DeclClass declClass) {
    int nComments = declClass.getComments();
    Vector vecDecl = declClass.getVecDecl();
    if (vecDecl == null)
      if (nComments > 0)
        return Float.MAX_VALUE;
      else
        return 0;

    int nfpb = 0, nfpr = 0;
    int nmpb = 0, nmpr = 0;
    int sizeVecDecl = vecDecl.size();
    for (int i=0; i<sizeVecDecl; i++) {
      Decl decl = (Decl)vecDecl.elementAt(i);
      if ((decl instanceof DeclConstant) || (decl instanceof DeclField)) {
        if (decl.isModifier(new ModProtected())) nfpr++;
        if (decl.isModifier(new ModPublic())) nfpb++;
      }

      if ( (decl instanceof DeclConstructor)
        || (decl instanceof DeclDestructor)
        || (decl instanceof DeclMethod)
        || (decl instanceof DeclOperator)) {
        if (decl.isModifier(new ModProtected())) nmpr++;
        if (decl.isModifier(new ModPublic())) nmpb++;
      }
    }

    return nComments / (float)(nmpb + nmpr + nfpb + nfpr);
  }

  public float AnalyzabilityMetric(DeclClass declClass) {
    Vector vecDecl = declClass.getVecDecl();
    if (vecDecl == null)
      return 0;

    int nm = 0;
    float sum_vg = 0;
    int sizeVecDecl = vecDecl.size();
    for (int i=0; i<sizeVecDecl; i++) {
      Decl decl = (Decl)vecDecl.elementAt(i);
      if ( (decl instanceof DeclConstructor)
        || (decl instanceof DeclDestructor)
        || (decl instanceof DeclMethod)
        || (decl instanceof DeclOperator)) {
        StmEmbedded body = decl.getStm();
        sum_vg += getVg(body);
        nm += 1;
      }
    }

    float cl_wmc = sum_vg/nm + 1;
    cl_wmc += COMLFMetric(declClass);

    if (declClass.isInherited())
      cl_wmc += 1;

    return cl_wmc;
  }

  public int ChangeabilityMetric(DeclClass declClass) {
    return UsabilityMetric(declClass) + SpecialityMetric(declClass);
  }

  public int UsabilityMetric(DeclClass declClass) {
    Vector vecDecl = declClass.getVecDecl();
    if (vecDecl == null)
        return 0;

    int nfpb = 0;
    int nmpb = 0;
    int sizeVecDecl = vecDecl.size();
    for (int i=0; i<sizeVecDecl; i++) {
      Decl decl = (Decl)vecDecl.elementAt(i);
      if ((decl instanceof DeclConstant) || (decl instanceof DeclField))
        if (decl.isModifier(new ModPublic())) nfpb++;

      if ( (decl instanceof DeclConstructor)
        || (decl instanceof DeclDestructor)
        || (decl instanceof DeclMethod)
        || (decl instanceof DeclOperator))
        if (decl.isModifier(new ModPublic())) nmpb++;
    }

    return nfpb * 2 + nmpb;
  }

  public int SpecialityMetric(DeclClass declClass) {
    Vector vecDecl = declClass.getVecDecl();
    if (vecDecl == null)
        return 0;

    int nfpb = 0, nfpr = 0;
    int nmpb = 0, nmpr = 0;
    int sizeVecDecl = vecDecl.size();
    for (int i=0; i<sizeVecDecl; i++) {
      Decl decl = (Decl)vecDecl.elementAt(i);
      if ((decl instanceof DeclConstant) || (decl instanceof DeclField)) {
        if (decl.isModifier(new ModProtected())) nfpr++;
        if (decl.isModifier(new ModPublic())) nfpb++;
      }

      if ( (decl instanceof DeclConstructor)
        || (decl instanceof DeclDestructor)
        || (decl instanceof DeclMethod)
        || (decl instanceof DeclOperator)) {
        if (decl.isModifier(new ModProtected())) nmpr++;
        if (decl.isModifier(new ModPublic())) nmpb++;
      }
    }

    int nResult = (nfpb + nfpr) * 2 + nmpb + nmpr;
    if (declClass.isInherited() == true)
      nResult += 10;

    return nResult;
  }

  public float COMLFMetric(Decl decl) {
    float nComments = decl.getComments();
    return nComments/getInstructions(decl.getStm());
  }

  private int getInstructions(Stm stm) {
    if (stm == null)
      return 0;

    int nInstr = 0;
    if (stm instanceof StmSwitch) {
      Vector vecSec = ((StmSwitch)stm).getVecSec();
      if (vecSec != null) {
        int sizeVecSec = vecSec.size();
        for (int i=0; i<sizeVecSec; i++) {
          SwitchSection sec = (SwitchSection)vecSec.elementAt(i);
          nInstr += sec.getVecLbl().size();
          Vector vecStm = sec.getVecStm();
          if (vecStm != null) {
            int sizeVecStm = vecStm.size();
            for (int j=0; j<sizeVecStm; j++) {
              Stm newStm = (Stm)vecStm.elementAt(j);
              nInstr += getInstructions(newStm);
            }
          }
        }
      }

      return nInstr + 1;
    }

    if (stm instanceof StmBlock) {
      Vector vecStm = stm.getVecStm();
      if (vecStm != null) {
        int sizeVecStm = vecStm.size();
        for (int i=0; i<sizeVecStm; i++) {
          Stm newStm = (Stm)vecStm.elementAt(i);
          nInstr += getInstructions(newStm);
        }
      }

      return nInstr;
    }

    if ((stm instanceof StmChecked)
      || (stm instanceof StmTry)
      || (stm instanceof StmUnChecked)
      || (stm instanceof StmUnsafe)) {
      Stm newStm = ((StmEmbedded)stm).getBlock();
      nInstr += getInstructions(newStm);

      if (stm instanceof StmTry) {
        FinallyClause fc = ((StmTry)stm).getFinallyClause();
        if (fc != null)
          nInstr += getInstructions(fc.getBlock());

        CatchClause cc = ((StmTry)stm).getCatchClause();
        if (cc != null) {
          nInstr += getInstructions(cc.getBlock());

          Vector vecSClause = cc.getVecSClause();
          if (vecSClause != null) {
            int sizeVecSClause = vecSClause.size();
            for (int i=0; i<sizeVecSClause; i++) {
              SpecificCatchClause scc = (SpecificCatchClause)vecSClause.elementAt(i);
              nInstr += getInstructions(scc.getBlock());
            }
          }
        }
      }

      return nInstr + 1;;
    }

    if (stm instanceof StmIf) {
      Stm stmT = ((StmIf)stm).getStmThen();
      Stm stmE = ((StmIf)stm).getStmElse();
      nInstr += getInstructions(stmT) + getInstructions(stmE);

      return nInstr + 1;
    }

    if ((stm instanceof StmFixed)
      || (stm instanceof StmIteration)
      || (stm instanceof StmLabeled)
      || (stm instanceof StmLock)
      || (stm instanceof StmUsing)) {
      Stm newStm = stm.getStm();
      nInstr += getInstructions(newStm);

      return nInstr + 1;
    }

    // is StmEmpty or StmJump or StmThrow
    return 1;
  }

  public int Testab(DeclClass declClass) {
    Vector vecDecl = declClass.getVecDecl();
    if (vecDecl == null)
      return 0;

    int TESTAB = 0;
    int sizeVecDecl = vecDecl.size();
    for (int i=0; i<sizeVecDecl; i++) {
      Decl decl = (Decl)vecDecl.elementAt(i);
      if ( (decl instanceof DeclConstructor)
        || (decl instanceof DeclDestructor)
        || (decl instanceof DeclMethod)
        || (decl instanceof DeclOperator))
        if (decl.isModifier(new ModPublic())
          || decl.isModifier(new ModPrivate())
          || decl.isModifier(new ModProtected())) {
          StmEmbedded body = decl.getStm();
          TESTAB += getPaths(body);
        }
    }

    return TESTAB;
  }

  public float TestabilityMetric() {
    Vector vecDeclType = parse_tree.getVecDecl();

    if (vecDeclType == null)
      return 0;

    int nm = 0;
    float sum_vg = 0;
    int sizeVecDeclType = vecDeclType.size();
    for (int i=0; i<sizeVecDeclType; i++) {
      Decl decl = (Decl)vecDeclType.elementAt(i);
      if (decl instanceof DeclClass) {
        nm += getMethods((DeclClass)decl);
        sum_vg += getSumVgClass((DeclClass)decl);
      }

      if ( (decl instanceof DeclConstructor)
        || (decl instanceof DeclDestructor)
        || (decl instanceof DeclMethod)
        || (decl instanceof DeclOperator)) {
        StmEmbedded body = decl.getStm();
        nm += 1;
        sum_vg += getVg(body) + 1;
      }
    }

    return sum_vg/nm;
  }

  private int getMethods(DeclClass declClass) {
    Vector vecDecl = declClass.getVecDecl();
    if (vecDecl == null)
      return 0;

    int nMethods = 0;
    int sizeVecDecl = vecDecl.size();
    for (int i=0; i<sizeVecDecl; i++) {
      Decl decl = (Decl)vecDecl.elementAt(i);
      if ( (decl instanceof DeclConstructor)
        || (decl instanceof DeclDestructor)
        || (decl instanceof DeclMethod)
        || (decl instanceof DeclOperator))
        nMethods += 1;

      if (decl instanceof DeclClass)
        nMethods += getMethods((DeclClass)decl);
    }

    return nMethods;
  }

  private int getSumVgClass(DeclClass declClass) {
    Vector vecDecl = declClass.getVecDecl();
    if (vecDecl == null)
      return 0;

    int sum_vg = 0;
    int sizeVecDecl = vecDecl.size();
    for (int i=0; i<sizeVecDecl; i++) {
      Decl decl = (Decl)vecDecl.elementAt(i);
      if ( (decl instanceof DeclConstructor)
        || (decl instanceof DeclDestructor)
        || (decl instanceof DeclMethod)
        || (decl instanceof DeclOperator)) {
        StmEmbedded body = decl.getStm();
        sum_vg += getVg(body) + 1;
      }

      if (decl instanceof DeclClass)
        sum_vg += getSumVgClass((DeclClass)decl);
    }

    return sum_vg;
  }

  public int StabilityMetric(DeclClass declClass) {
    return getClassInheritors(parse_tree.getVecDecl(), declClass)
      + getClassUsers(parse_tree.getVecDecl(), declClass);
  }

  private int getClassInheritors(Vector vecDecl, DeclClass declClass) {
    if (vecDecl == null)
      return 0;

    int in_noc = 0;
    int sizeVecDecl = vecDecl.size();
    for (int i=0; i<sizeVecDecl; i++) {
      Decl decl = (Decl)vecDecl.elementAt(i);
      if (decl instanceof DeclClass) {
        if (!((DeclClass)decl).equals(declClass) && ((DeclClass)decl).isInherited()) {
          ClassBase cb = ((DeclClass)decl).getClassBase();
          if (cb.getType().equals(new TypeName(declClass.getName())))
            in_noc += 1;
        }

        Vector vecDeclClass = ((DeclClass)decl).getVecDecl();
        in_noc += getClassInheritors(vecDeclClass, declClass);
      }
    }

    return in_noc;
  }

  private int getClassUsers(Vector vecDecl, DeclClass declClass) {
    if (vecDecl == null)
      return 0;

    int cd_users = 0;
    int sizeVecDecl = vecDecl.size();
    for (int i=0; i<sizeVecDecl; i++) {
      Decl decl = (Decl)vecDecl.elementAt(i);

      Type type;
      if (decl instanceof DeclConstant) {
        type = ((DeclConstant)decl).getType();
        if (type.equals(new TypeName(declClass.getName())))
          cd_users += 1;
      }
      else
        if (decl instanceof DeclField) {
          type = ((DeclField)decl).getType();
          if (type.equals(new TypeName(declClass.getName())))
            cd_users += 1;
        }
        else
          if ((decl instanceof DeclClass) && !((DeclClass)decl).equals(declClass)) {
            Vector vecDeclClass = ((DeclClass)decl).getVecDecl();
            cd_users += getClassUsers(vecDeclClass, declClass);
          }
    }

    return cd_users;
  }

  public int TestabilityMetric(DeclClass declClass) {
    int nResult = Testab(declClass) + getClassUsed(declClass);
    if (declClass.isInherited())
      nResult += 1;

    return nResult;
  }

  private int getClassUsed(DeclClass declClass) {
    Vector vecDecl = declClass.getVecDecl();
    if (vecDecl == null)
      return 0;

    int cd_used = 0;
    int sizeVecDecl = vecDecl.size();
    for (int i=0; i<sizeVecDecl; i++) {
      Decl decl = (Decl)vecDecl.elementAt(i);

      Type type;
      if (decl instanceof DeclConstant) {
        type = ((DeclConstant)decl).getType();
        if (type.equals(new TypeObject()) || type.equals(new TypeString())
          || (type instanceof TypeName))
          cd_used += 1;
      }
      else
        if (decl instanceof DeclField) {
          type = ((DeclField)decl).getType();
          if (type.equals(new TypeObject()) || type.equals(new TypeString())
            || (type instanceof TypeName))
            cd_used += 1;
        }
    }

    return cd_used;
  }
}