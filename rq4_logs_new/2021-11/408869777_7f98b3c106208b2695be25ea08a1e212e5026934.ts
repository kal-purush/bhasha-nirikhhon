import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

const seed = async () => {
  const mediaIcons = [
    {
      name: 'Discord',
      iconUrl: 'https://svgshare.com/i/btZ.svg',
    },
    {
      name: 'Facebook',
      iconUrl: 'https://svgshare.com/i/c4g.svg',
    },
    {
      name: 'Linkedin',
      iconUrl: 'https://svgshare.com/i/c4f.svg',
    },
    {
      name: 'Github',
      iconUrl: 'https://svgshare.com/i/c5s.svg',
    },
  ];
  const skills = [
    {
      name: 'Node',
    },

    {
      name: 'React',
    },

    {
      name: 'Figma',
    },
  ];

  // Create media Icons
  await Promise.all(
    mediaIcons.map((m, i) => {
      return prisma.mediaIcon.create({
        data: {
          name: m.name,
          iconUrl: m.iconUrl,
        },
      });
    })
  );

  // Create Skills
  await Promise.all(
    skills.map((s, i) => {
      return prisma.skills.create({
        data: {
          name: s.name,
        },
      });
    })
  );
};

seed()
  .catch((e) => {
    // eslint-disable-next-line no-console
    console.error(e); //show back-end error
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
    console.log('✅ All done !');
  });