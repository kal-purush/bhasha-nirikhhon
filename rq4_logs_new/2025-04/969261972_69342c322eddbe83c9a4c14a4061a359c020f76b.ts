import { NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

const GROUPS_FILE_PATH = path.join(process.cwd(), 'app/data/mockGroups.ts');

function updateGroupsFile(groups: any[]) {
  const fileContent = `export interface Group {
  id: string;
  name: string;
  members: string[]; // Array of user IDs
  teacherId: string;
}

export const mockGroups: Group[] = ${JSON.stringify(groups, null, 2)};`;

  fs.writeFileSync(GROUPS_FILE_PATH, fileContent);
}

export async function POST(request: Request) {
  try {
    const { action, data } = await request.json();
    
    // Read current groups
    const fileContent = fs.readFileSync(GROUPS_FILE_PATH, 'utf-8');
    const match = fileContent.match(/export const mockGroups: Group\[] = (\[[\s\S]*?\]);/);
    let currentGroups = [];
    
    if (match) {
      currentGroups = eval(match[1]);
    }

    let updatedGroups;
    
    switch (action) {
      case 'create':
        const newGroup = {
          ...data,
          id: (currentGroups.length + 1).toString()
        };
        updatedGroups = [...currentGroups, newGroup];
        break;
        
      case 'update':
        updatedGroups = currentGroups.map((group: any) =>
          group.id === data.id ? { ...group, ...data } : group
        );
        break;
        
      case 'delete':
        updatedGroups = currentGroups.filter((group: any) => group.id !== data.id);
        break;
        
      default:
        return NextResponse.json({ error: 'Invalid action' }, { status: 400 });
    }

    updateGroupsFile(updatedGroups);
    
    return NextResponse.json({ success: true, groups: updatedGroups });
  } catch (error) {
    console.error('Error handling groups:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
} 