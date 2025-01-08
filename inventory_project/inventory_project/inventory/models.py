from django.db import models

class Product(models.Model):
    name = models.CharField(max_length=100)
    quantity = models.IntegerField(default=0)
    price = models.DecimalField(max_digits=10, decimal_places=2)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name


class InventoryLog(models.Model):
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    action = models.CharField(max_length=50)  # 'ADD', 'UPDATE', 'DELETE'
    quantity_changed = models.IntegerField()
    timestamp = models.DateTimeField(auto_now_add=True)
