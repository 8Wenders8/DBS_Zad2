from django.urls import path
from . import views

# URL Conf
urlpatterns = [
    path('health/', views.health)
]