const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

// 初始化Express应用
const app = express();
const PORT = 3001;

// 中间件
app.use(express.json({ limit: '1mb' }));
app.use(cors({
  origin: ['http://localhost:5500', 'http://127.0.0.1:5500'],
  methods: ['GET', 'POST', 'DELETE'],
  credentials: true
}));

// 数据文件路径
const DATA_FILE = path.join(__dirname, 'templates.json');

// 富士滤镜预设
const fujiPresets = [
    {
        id: 'fuji-provia',
        name: {
            zh: '富士 Provia 标准',
            en: 'Fuji Provia Standard'
        },
        preset: true,
        params: {
            dynamicRange: 30,
            highlights: -15,
            shadows: 20,
            vibrance: 25,
            sharpness: 20,
            grain: 10,
            colorEffect: 10,
            blueEffect: -5,
            whiteBalance: -8,
            noiseReduction: 15
        }
    },
    {
        id: 'fuji-velvia',
        name: {
            zh: '富士 Velvia 鲜艳',
            en: 'Fuji Velvia Vivid'
        },
        preset: true,
        params: {
            dynamicRange: 40,
            highlights: -10,
            shadows: 15,
            vibrance: 65,
            sharpness: 25,
            grain: 15,
            colorEffect: 30,
            blueEffect: 10,
            whiteBalance: -5,
            noiseReduction: 10
        }
    },
    {
        id: 'fuji-astia',
        name: {
            zh: '富士 Astia 柔和',
            en: 'Fuji Astia Soft'
        },
        preset: true,
        params: {
            dynamicRange: 25,
            highlights: -20,
            shadows: 30,
            vibrance: 10,
            sharpness: 15,
            grain: 5,
            colorEffect: 5,
            blueEffect: -15,
            whiteBalance: 5,
            noiseReduction: 20
        }
    },
    {
        id: 'fuji-classic-chrome',
        name: {
            zh: '富士 Classic Chrome',
            en: 'Fuji Classic Chrome'
        },
        preset: true,
        params: {
            dynamicRange: 35,
            highlights: -25,
            shadows: 25,
            vibrance: -10,
            sharpness: 15,
            grain: 20,
            colorEffect: -15,
            blueEffect: 5,
            whiteBalance: -10,
            noiseReduction: 15
        }
    },
    {
        id: 'fuji-acros',
        name: {
            zh: '富士 Acros 黑白',
            en: 'Fuji Acros B&W'
        },
        preset: true,
        params: {
            dynamicRange: 40,
            highlights: -30,
            shadows: 25,
            vibrance: -100,
            sharpness: 30,
            grain: 25,
            colorEffect: 0,
            blueEffect: 0,
            whiteBalance: 0,
            noiseReduction: 10
        }
    },
    {
        id: 'fuji-pro-neg-hi',
        name: {
            zh: '富士 Pro Neg Hi',
            en: 'Fuji Pro Neg Hi'
        },
        preset: true,
        params: {
            dynamicRange: 30,
            highlights: -20,
            shadows: 15,
            vibrance: 15,
            sharpness: 20,
            grain: 8,
            colorEffect: 5,
            blueEffect: -3,
            whiteBalance: -5,
            noiseReduction: 15
        }
    },
    {
        id: 'fuji-pro-neg-std',
        name: {
            zh: '富士 Pro Neg Std',
            en: 'Fuji Pro Neg Standard'
        },
        preset: true,
        params: {
            dynamicRange: 25,
            highlights: -15,
            shadows: 10,
            vibrance: 5,
            sharpness: 15,
            grain: 5,
            colorEffect: 0,
            blueEffect: 0,
            whiteBalance: 0,
            noiseReduction: 15
        }
    },
    {
        id: 'fuji-eterna',
        name: {
            zh: '富士 Eterna 电影',
            en: 'Fuji Eterna Cinema'
        },
        preset: true,
        params: {
            dynamicRange: 45,
            highlights: -35,
            shadows: 35,
            vibrance: -5,
            sharpness: 10,
            grain: 20,
            colorEffect: -10,
            blueEffect: 5,
            whiteBalance: -5,
            noiseReduction: 10
        }
    },
    {
        id: 'fuji-sepia',
        name: {
            zh: '富士 Sepia 怀旧',
            en: 'Fuji Sepia Vintage'
        },
        preset: true,
        params: {
            dynamicRange: 20,
            highlights: -15,
            shadows: 15,
            vibrance: -50,
            sharpness: 5,
            grain: 35,
            colorEffect: -25,
            blueEffect: -30,
            whiteBalance: 30,
            noiseReduction: 5
        }
    },
    {
        id: 'fuji-superia',
        name: {
            zh: '富士 Superia 400',
            en: 'Fuji Superia 400'
        },
        preset: true,
        params: {
            dynamicRange: 35,
            highlights: -10,
            shadows: 25,
            vibrance: 40,
            sharpness: 20,
            grain: 30,
            colorEffect: 15,
            blueEffect: 5,
            whiteBalance: 0,
            noiseReduction: 20
        }
    },
    {
        id: 'fuji-pro400h',
        name: {
            zh: '富士 Pro 400H',
            en: 'Fuji Pro 400H'
        },
        preset: true,
        params: {
            dynamicRange: 30,
            highlights: -20,
            shadows: 20,
            vibrance: 25,
            sharpness: 15,
            grain: 25,
            colorEffect: 5,
            blueEffect: -5,
            whiteBalance: 10,
            noiseReduction: 15
        }
    },
    {
        id: 'fuji-neopan',
        name: {
            zh: '富士 Neopan 黑白',
            en: 'Fuji Neopan B&W'
        },
        preset: true,
        params: {
            dynamicRange: 40,
            highlights: -35,
            shadows: 35,
            vibrance: -100,
            sharpness: 25,
            grain: 40,
            colorEffect: 0,
            blueEffect: 0,
            whiteBalance: 0,
            noiseReduction: 10
        }
    },
    {
        id: 'fuji-xpro',
        name: {
            zh: '富士 XPro 交叉冲洗',
            en: 'Fuji XPro Cross Process'
        },
        preset: true,
        params: {
            dynamicRange: 35,
            highlights: -15,
            shadows: 20,
            vibrance: 30,
            sharpness: 20,
            grain: 35,
            colorEffect: 40,
            blueEffect: 15,
            whiteBalance: -15,
            noiseReduction: 15
        }
    },
    {
        id: 'fuji-reala',
        name: {
            zh: '富士 Reala 100',
            en: 'Fuji Reala 100'
        },
        preset: true,
        params: {
            dynamicRange: 25,
            highlights: -10,
            shadows: 15,
            vibrance: 20,
            sharpness: 15,
            grain: 10,
            colorEffect: 5,
            blueEffect: 0,
            whiteBalance: 0,
            noiseReduction: 10
        }
    },
    {
        id: 'fuji-fortia',
        name: {
            zh: '富士 Fortia 超饱和',
            en: 'Fuji Fortia Supersaturated'
        },
        preset: true,
        params: {
            dynamicRange: 40,
            highlights: -15,
            shadows: 15,
            vibrance: 80,
            sharpness: 25,
            grain: 20,
            colorEffect: 45,
            blueEffect: 15,
            whiteBalance: -5,
            noiseReduction: 10
        }
    }
];

// 确保数据文件存在并更新预设格式
function ensureDataFile() {
  console.log('检查数据文件:', DATA_FILE);
  
  if (!fs.existsSync(DATA_FILE)) {
    console.log('创建新的数据文件');
    try {
      // 初始数据使用富士预设
      fs.writeFileSync(DATA_FILE, JSON.stringify(fujiPresets, null, 2));
      console.log('数据文件创建成功');
    } catch (error) {
      console.error('创建数据文件失败:', error);
    }
  } else {
    console.log('数据文件已存在，检查预设格式是否需要更新');
    try {
      // 读取现有数据
      const data = fs.readFileSync(DATA_FILE, 'utf8');
      let templates = JSON.parse(data);
      
      // 检查是否需要更新预设模板
      let needsUpdate = false;
      
      templates.forEach((template, index) => {
        if (template.preset && typeof template.name === 'string') {
          // 找到对应的新预设
          const newPreset = fujiPresets.find(p => p.id === template.id);
          if (newPreset) {
            templates[index].name = newPreset.name;
            needsUpdate = true;
          }
        }
      });
      
      // 如果有更新，保存回文件
      if (needsUpdate) {
        console.log('更新预设模板格式为多语言');
        fs.writeFileSync(DATA_FILE, JSON.stringify(templates, null, 2));
      }
    } catch (error) {
      console.error('更新模板格式失败:', error);
    }
  }
}

// 读取模板数据
function readTemplates() {
    ensureDataFile();
    const data = fs.readFileSync(DATA_FILE, 'utf8');
    return JSON.parse(data);
}

// 写入模板数据
function writeTemplates(templates) {
    fs.writeFileSync(DATA_FILE, JSON.stringify(templates));
}

// 获取所有模板
app.get('/api/templates', (req, res) => {
    try {
        const templates = readTemplates();
        res.json(templates);
    } catch (error) {
        console.error('读取模板失败:', error);
        res.status(500).json({ error: '服务器错误' });
    }
});

// 创建新模板
app.post('/api/templates', (req, res) => {
    try {
        const templates = readTemplates();
        const userTemplates = templates.filter(t => !t.preset);
        
        // 检查是否达到最大数量限制
        if (userTemplates.length >= 15) {
            return res.status(400).json({ error: '已达到最大模板数量限制' });
        }
        
        const newTemplate = {
            id: uuidv4(),
            name: req.body.name,
            params: req.body.params,
            preset: false,
            created: new Date().toISOString()
        };
        
        templates.push(newTemplate);
        writeTemplates(templates);
        
        res.status(201).json(newTemplate);
    } catch (error) {
        console.error('创建模板失败:', error);
        res.status(500).json({ error: '服务器错误' });
    }
});

// 删除模板
app.delete('/api/templates/:id', (req, res) => {
    try {
        const templates = readTemplates();
        const template = templates.find(t => t.id === req.params.id);
        
        if (!template) {
            return res.status(404).json({ error: '模板不存在' });
        }
        
        // 不允许删除预设模板
        if (template.preset) {
            return res.status(400).json({ error: '不能删除预设模板' });
        }
        
        const filteredTemplates = templates.filter(t => t.id !== req.params.id);
        writeTemplates(filteredTemplates);
        
        res.status(200).json({ message: '模板已删除' });
    } catch (error) {
        console.error('删除模板失败:', error);
        res.status(500).json({ error: '服务器错误' });
    }
});

// 启动服务器
app.listen(PORT, () => {
    console.log(`服务器运行在 http://localhost:${PORT}`);
    ensureDataFile();
}); 