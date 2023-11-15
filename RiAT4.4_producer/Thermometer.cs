using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RiAT4._4_producer
{
    public class Thermometer
    {
        public readonly string name;
        const double maxValue = 100;
        const double minValue = 0;
        double value = 70;
        Action<Thermometer> valueChangedHandler;
        public double Value
        {
            private set
            {
                this.value = value;
                valueChangedHandler(this);
            }
            get => this.value;
        }
        public Thermometer(string name,Action<Thermometer> thermometerValueChangedHandler)
        {
            valueChangedHandler = thermometerValueChangedHandler;
            this.name = name;
        }
        public async Task Start()
        {
            Random rnd = new Random();

            while (true)
            {
                await Task.Delay(100);

                if (rnd.Next(0, 1) == 1 && value < maxValue - 0.1)
                    Value += rnd.NextDouble();
                else if (value > minValue + 0.1)
                    Value -= rnd.NextDouble();
            }
        }
    }
}