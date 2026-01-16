from django.core.management.base import BaseCommand
from servicemap.models import Service, Host, Role, HostRole, User, Deployment


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        s1, is_new = Service.objects.get_or_create(name="Demo Service 1")
        s1.notes = "This is a service..."
        s1.save()
        s2, is_new = Service.objects.get_or_create(name="Demo 2")
        s3, is_new = Service.objects.get_or_create(name="Service 3 (full)")
        s3.notes = "This one has notes"
        s3.save()

        h1, is_new = Host.objects.get_or_create(name="app-01.example.com")
        h2, is_new = Host.objects.get_or_create(name="app-02.example.com")
        h3, is_new = Host.objects.get_or_create(name="app-03.example.com")
        h4, is_new = Host.objects.get_or_create(name="app-04.example.com")
        h5, is_new = Host.objects.get_or_create(name="db-01.example.com")
        h6, is_new = Host.objects.get_or_create(name="db-02.example.com")
        h7, is_new = Host.objects.get_or_create(name="db-03.example.com")
        h8, is_new = Host.objects.get_or_create(name="db-04.example.com")
        h9, is_new = Host.objects.get_or_create(name="other.example.com")
        h10, is_new = Host.objects.get_or_create(name="deploy1.example.com")
        h11, is_new = Host.objects.get_or_create(name="deploy2.example.com")

        r1, is_new = Role.objects.get_or_create(name="application")
        r2, is_new = Role.objects.get_or_create(name="database")
        r3, is_new = Role.objects.get_or_create(name="database-master")
        r4, is_new = Role.objects.get_or_create(name="database-slave")
        r5, is_new = Role.objects.get_or_create(name="rdis")

        hr1, x = HostRole.objects.get_or_create(host=h1, role=r1)
        hr2, x = HostRole.objects.get_or_create(host=h2, role=r1)
        hr3, x = HostRole.objects.get_or_create(host=h3, role=r1)
        hr4, x = HostRole.objects.get_or_create(host=h4, role=r1)
        hr5, x = HostRole.objects.get_or_create(host=h5, role=r2)
        hr6, x = HostRole.objects.get_or_create(host=h6, role=r3)
        hr7, x = HostRole.objects.get_or_create(host=h7, role=r4)
        hr8, x = HostRole.objects.get_or_create(host=h8, role=r4)
        hr9, x = HostRole.objects.get_or_create(host=h9, role=r5)

        s1.hostroles.clear()
        s1.hostroles.add(hr1)
        s1.save()

        s2.hostroles.clear()
        s2.hostroles.add(hr2)
        s2.prereqs.clear()
        s2.prereqs.add(s1)
        s2.save()

        s3.hostroles.clear()
        s3.hostroles.add(hr3)
        s3.hostroles.add(hr4)
        s3.hostroles.add(hr5)
        s3.hostroles.add(hr6)
        s3.hostroles.add(hr7)
        s3.hostroles.add(hr8)
        s3.hostroles.add(hr9)

        s3.prereqs.clear()
        s3.prereqs.add(s1)
        s3.prereqs.add(s2)
        s3.save()

        u1, is_new = User.objects.get_or_create(login="deployer1")
        u2, is_new = User.objects.get_or_create(login="deployer2")

        Deployment.objects.get_or_create(service=s3, deployed_by=u1,
                                         deployed_from=h10)

        Deployment.objects.get_or_create(service=s3, deployed_by=u2,
                                         deployed_from=h10)

        Deployment.objects.get_or_create(service=s3, deployed_by=u1,
                                         deployed_from=h10)

        Deployment.objects.get_or_create(service=s3, deployed_by=u2,
                                         deployed_from=h10)

        Deployment.objects.get_or_create(service=s3, deployed_by=u2,
                                         deployed_from=h11)

        Deployment.objects.get_or_create(service=s2, deployed_by=u1,
                                         deployed_from=h10)

        Deployment.objects.get_or_create(service=s2, deployed_by=u2,
                                         deployed_from=h10)

        Deployment.objects.get_or_create(service=s2, deployed_by=u2,
                                         deployed_from=h11)